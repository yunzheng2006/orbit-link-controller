package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The subcommand is taken off the arguments before the flags are parsed.
// It once took the flag after it as the subcommand ("unknown command
// --controller-deployment=…") and the Helm cleanup hook failed.
func TestSubcommandWithFlags(t *testing.T) {
	if os.Getenv("OLC_CHILD") == "1" {
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestSubcommandWithFlags", "--")
	cmd.Args = []string{os.Args[0], "version", "--cluster-name=x"}
	cmd.Env = append(os.Environ(), "OLC_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "orbit-link-controller "+version) {
		t.Fatalf("%v: %s", err, out)
	}
}
