// Package testname tries to figure out the current test name from the stack
package testname

import (
	"fmt"
	"strings"

	"github.com/facebookgo/stack"
)

var replacer = strings.NewReplacer(".", "_", "/", "_", ":", "_")

// Get finds the name of test and the package from the stack, if possible, or tries its best otherwise.
func Get(id string) string {
	s := stack.Callers(1)

	for _, f := range s {
		if strings.HasSuffix(f.File, "_test.go") {
			var name string
			if strings.HasPrefix(f.Name, "Test") {
				name = f.Name
			} else if strings.HasSuffix(f.Name, "SetUpSuite") {
				name = "SetUpSuite"
			} else if strings.HasSuffix(f.Name, "SetUpTest") {
				name = "SetUpTest"
			}
			if name != "" {
				file := replacer.Replace(f.File)
				return fmt.Sprintf("%s_%s_", file, name)
			}
		}
	}

	// find the first caller outside ourselves
	outside := s[0].File
	for _, f := range s {
		if f.File != s[0].File {
			outside = f.Name
			break
		}
	}

	return fmt.Sprintf("TestNameNotFound_%s_%s_", id, outside)
}
