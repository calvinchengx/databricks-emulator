// Package gitcmd runs git. Repos clone and update a real remote; this is
// the engine, not a file copy.
package gitcmd

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// ErrGitMissing is returned when git is not on PATH.
var ErrGitMissing = fmt.Errorf("git is not on PATH; Repos clone a real remote")

// LookPath is exec.LookPath("git"). Tests and the HTTP layer both use it
// so a missing binary is named, never a silent empty repo.
func LookPath() (string, error) {
	p, err := exec.LookPath("git")
	if err != nil {
		return "", ErrGitMissing
	}
	return p, nil
}

func gitEnv() []string {
	return append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
	)
}

func run(ctx context.Context, dir string, args ...string) (string, error) {
	bin, err := LookPath()
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	cmd.Env = gitEnv()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(stdout.String())
		}
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", args[0], msg)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// withAuth puts username/token on http(s) URLs. file:// and git:// are
// unchanged — a token does not belong on those.
func withAuth(raw, user, token string) (string, error) {
	if user == "" && token == "" {
		return raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("git remote url: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		u.User = url.UserPassword(user, token)
		return u.String(), nil
	default:
		return raw, nil
	}
}

// Clone copies a remote into dest. dest must not already exist. Credentials
// are used for the clone then stripped from origin so the working tree does
// not keep the token.
func Clone(ctx context.Context, remote, dest, user, token string) error {
	authed, err := withAuth(remote, user, token)
	if err != nil {
		return err
	}
	if _, err := run(ctx, "", "clone", "--", authed, dest); err != nil {
		return err
	}
	if authed != remote {
		if _, err := run(ctx, dest, "remote", "set-url", "origin", remote); err != nil {
			return err
		}
	}
	return nil
}

// Update fetches and checks out branch, tag, or the current branch's tip.
// Tag and branch together is the caller's problem to refuse.
func Update(ctx context.Context, dest, branch, tag, user, token string) error {
	origin, err := run(ctx, dest, "remote", "get-url", "origin")
	if err != nil {
		return err
	}
	authed, err := withAuth(origin, user, token)
	if err != nil {
		return err
	}
	if authed != origin {
		if _, err := run(ctx, dest, "remote", "set-url", "origin", authed); err != nil {
			return err
		}
		defer func() { _, _ = run(ctx, dest, "remote", "set-url", "origin", origin) }()
	}
	if _, err := run(ctx, dest, "fetch", "--tags", "origin"); err != nil {
		return err
	}
	switch {
	case tag != "":
		_, err := run(ctx, dest, "checkout", "--detach", "tags/"+tag)
		return err
	case branch != "":
		_, err := run(ctx, dest, "checkout", "-B", branch, "origin/"+branch)
		return err
	default:
		cur, err := run(ctx, dest, "rev-parse", "--abbrev-ref", "HEAD")
		if err != nil {
			return err
		}
		if cur == "HEAD" {
			return fmt.Errorf("git update: detached HEAD; name a branch or tag")
		}
		_, err = run(ctx, dest, "merge", "--ff-only", "origin/"+cur)
		return err
	}
}

// Head returns the current commit and branch (empty branch when detached).
func Head(ctx context.Context, dest string) (sha, branch string, err error) {
	sha, err = run(ctx, dest, "rev-parse", "HEAD")
	if err != nil {
		return "", "", err
	}
	branch, err = run(ctx, dest, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "", "", err
	}
	if branch == "HEAD" {
		branch = ""
	}
	return sha, branch, nil
}
