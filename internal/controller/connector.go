package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ConnectorSpec is the connector Deployment: the same shape as the
// orbit-link Helm chart (non-root, read-only root, no capabilities, token as
// a file, exec probes on the loopback health endpoint, spread across nodes).
func ConnectorSpec(c *Config, labels map[string]string, secretRev string) appsv1.DeploymentSpec {
	sel := map[string]string{"app.kubernetes.io/name": labels["app.kubernetes.io/name"], "app.kubernetes.io/instance": labels["app.kubernetes.io/instance"],
		"app.kubernetes.io/component": "connector"}
	replicas := c.Replicas
	no, yes := false, true
	uid := int64(65532)
	grace := int64(13)
	mode := int32(0o400)
	maxUnavail, maxSurge := intstr.FromInt32(0), intstr.FromInt32(1)
	hc := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/orbit-link", "-healthcheck"}}}, TimeoutSeconds: 5}
	startup, ready := *hc, *hc
	startup.PeriodSeconds, startup.FailureThreshold = 5, 24
	ready.PeriodSeconds, ready.FailureThreshold = 10, 3
	env := []corev1.EnvVar{
		{Name: "ORBIT_LINK_ENDPOINT", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: c.ConnectorName()}, Key: "endpoint"}}},
		{Name: "ORBIT_LINK_FINGERPRINT", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: c.ConnectorName()}, Key: "fingerprint"}}},
		{Name: "ORBIT_LINK_LOCAL", Value: c.RouterURL},
		{Name: "ORBIT_LINK_TOKEN_FILE", Value: "/var/run/orbit-link/token"},
		{Name: "ORBIT_LINK_HEALTH", Value: "127.0.0.1:9901"},
		{Name: "ORBIT_LINK_NO_UPDATE", Value: "1"},
		{Name: "ORBIT_LINK_DRAIN", Value: "8s"},
	}
	var args []string
	if c.ConnectorTCP {
		args = []string{"-tcp"}
	}
	spread := func(key string, when corev1.UnsatisfiableConstraintAction) corev1.TopologySpreadConstraint {
		return corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: key, WhenUnsatisfiable: when, LabelSelector: &metav1.LabelSelector{MatchLabels: sel}}
	}
	return appsv1.DeploymentSpec{
		Replicas:             &replicas,
		RevisionHistoryLimit: ptr32(3),
		Selector:             &metav1.LabelSelector{MatchLabels: sel},
		Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: &maxUnavail, MaxSurge: &maxSurge}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{"link.yunzheng.space/credential-rev": secretRev}},
			Spec: corev1.PodSpec{
				AutomountServiceAccountToken:  &no,
				EnableServiceLinks:            &no,
				TerminationGracePeriodSeconds: &grace,
				SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid,
					SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
				Containers: []corev1.Container{{
					Name:            "connector",
					Image:           c.ConnectorImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Args:            args,
					Env:             env,
					VolumeMounts:    []corev1.VolumeMount{{Name: "token", MountPath: "/var/run/orbit-link", ReadOnly: true}},
					StartupProbe:    &startup,
					ReadinessProbe:  &ready,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("24Mi")},
						Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
					},
					SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
						Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				}},
				Volumes: []corev1.Volume{{Name: "token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: c.ConnectorName(), DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: "token", Path: "token"}}}}}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					spread("kubernetes.io/hostname", corev1.ScheduleAnyway),
					spread("topology.kubernetes.io/zone", corev1.ScheduleAnyway),
				},
			},
		},
	}
}

func ptr32(v int32) *int32 { return &v }
