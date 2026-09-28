package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/yunzheng2006/orbit-link-controller/internal/orbit"
)

// Cleanup undoes an installation (Helm pre-delete hook): stop the controller
// so nothing re-creates what is removed, remove every hostname this
// installation published, delete its tunnel, the connector and its Secret,
// and release our finalizers so nothing is left stuck.
func Cleanup(ctx context.Context, c client.Client, o *orbit.Client, cfg *Config, controllerDeployment string, logf func(string, ...any)) error {
	if controllerDeployment != "" {
		var d appsv1.Deployment
		key := types.NamespacedName{Namespace: cfg.Namespace, Name: controllerDeployment}
		if err := c.Get(ctx, key, &d); err == nil {
			patch := client.MergeFrom(d.DeepCopy())
			zero := int32(0)
			d.Spec.Replicas = &zero
			if err := c.Patch(ctx, &d, patch); err != nil {
				return fmt.Errorf("stopping the controller: %w", err)
			}
			for i := 0; i < 60; i++ {
				if err := c.Get(ctx, key, &d); err != nil || d.Status.Replicas == 0 {
					break
				}
				time.Sleep(time.Second)
			}
			logf("controller stopped")
		}
	}
	var sec corev1.Secret
	var id int64
	if err := c.Get(ctx, types.NamespacedName{Namespace: cfg.Namespace, Name: cfg.ConnectorName()}, &sec); err == nil {
		id, _ = strconv.ParseInt(string(sec.Data["tunnel-id"]), 10, 64)
	}
	if id == 0 {
		list, err := o.Tunnels(ctx)
		if err != nil {
			return err
		}
		for _, t := range list {
			if t.Name == cfg.TunnelName {
				id = t.ID
			}
		}
	}
	var errs []error
	if id > 0 {
		res, err := o.SyncHostnames(ctx, id, cfg.ManagedBy(), nil, false)
		switch {
		case orbit.IsNotFound(err):
		case err != nil:
			return fmt.Errorf("removing hostnames: %w", err)
		default:
			logf("removed hostnames %v", res.Removed)
			if err := o.DeleteTunnel(ctx, id); err != nil && !orbit.IsNotFound(err) {
				// A site made by hand still pointing at the tunnel: say so and
				// leave the tunnel; everything this installation made is gone.
				errs = append(errs, fmt.Errorf("deleting tunnel %d: %w", id, err))
			} else {
				logf("deleted tunnel %d", id)
			}
		}
	}
	for _, obj := range []client.Object{
		&appsv1.Deployment{}, &corev1.Secret{},
	} {
		obj.SetNamespace(cfg.Namespace)
		obj.SetName(cfg.ConnectorName())
		if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
		}
	}
	release := func(obj client.Object) {
		if !controllerutil.ContainsFinalizer(obj, Finalizer) {
			return
		}
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		controllerutil.RemoveFinalizer(obj, Finalizer)
		if err := c.Patch(ctx, obj, patch); client.IgnoreNotFound(err) != nil {
			errs = append(errs, err)
		}
	}
	var il networkingv1.IngressList
	if err := c.List(ctx, &il); err == nil {
		for i := range il.Items {
			ing := &il.Items[i]
			if cfg.IsOurIngress(ing) && len(ing.Status.LoadBalancer.Ingress) > 0 {
				patch := client.MergeFrom(ing.DeepCopy())
				ing.Status.LoadBalancer.Ingress = nil
				_ = c.Status().Patch(ctx, ing, patch)
			}
			release(ing)
		}
	} else {
		errs = append(errs, err)
	}
	if cfg.GatewayAPI {
		var rl gatewayv1.HTTPRouteList
		if err := c.List(ctx, &rl); err == nil {
			for i := range rl.Items {
				release(&rl.Items[i])
			}
		}
		var gl gatewayv1.GatewayList
		if err := c.List(ctx, &gl); err == nil {
			for i := range gl.Items {
				release(&gl.Items[i])
			}
		}
	}
	return errors.Join(errs...)
}
