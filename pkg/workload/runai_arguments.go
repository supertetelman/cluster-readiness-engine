// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"fmt"
	"strings"
)

// Run:ai parses strings rather than argv and loses nested quotes/backslashes.
// Kubernetes expands these literal environment values into one argument instead.
func runaiCommandLine(field string, argv []string, env map[string]any) (string, error) {
	quoted := make([]string, len(argv))
	for i, arg := range argv {
		if strings.ContainsAny(arg, "'\\") {
			// Run:ai's environment map has no ordering for container-env references.
			for name := range env {
				if strings.Contains(arg, "$("+name+")") {
					return "", fmt.Errorf("native Run:ai complex arguments cannot reference container environment with $(...); use shell $VAR references where appropriate")
				}
			}
			name := fmt.Sprintf("NVCRE_%s_%d", strings.ToUpper(field), i)
			if _, exists := env[name]; exists {
				return "", fmt.Errorf("native Run:ai argument expansion requires reserved environment name %s", name)
			}
			env[name] = runaiValue(arg)
			arg = "$(" + name + ")"
		}
		quoted[i] = "'" + arg + "'"
	}
	return strings.Join(quoted, " "), nil
}
