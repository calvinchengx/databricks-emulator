package store

import (
	"os"
	"testing"
)

func TestOpenSeedsAndReloads(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !s.FreshSeed || s.AdminPAT == "" || s.OIDCSecret == "" {
		t.Fatalf("seed: %+v", s)
	}
	if _, ok := s.Identity.LookupPAT(s.AdminPAT); !ok {
		t.Fatal("admin PAT not stored")
	}
	if _, ok := s.Identity.LookupPAT("dev"); ok {
		t.Fatal("dev accepted")
	}
	s2, err := Open(dir, 200)
	if err != nil {
		t.Fatal(err)
	}
	if s2.FreshSeed || s2.AdminPAT != s.AdminPAT {
		t.Fatalf("reload: fresh=%v pat changed", s2.FreshSeed)
	}
}

func TestOpenRequiresDataDir(t *testing.T) {
	if _, err := Open("", 0); err == nil {
		t.Fatal("empty data dir")
	}
}

func TestWorkspaceAndDBFSAndSecrets(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Workspace.Put("/a.py", []byte("x"), ObjectNotebook, "PYTHON"); err != nil {
		t.Fatal(err)
	}
	b, obj, err := s.Workspace.Get("/a.py")
	if err != nil || string(b) != "x" || obj.ObjectType != ObjectNotebook {
		t.Fatalf("get %q %+v %v", b, obj, err)
	}
	if err := s.Workspace.Mkdir("/d"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Workspace.List("/"); err != nil {
		t.Fatal(err)
	}
	if err := s.Workspace.Delete("/a.py", false); err != nil {
		t.Fatal(err)
	}

	if err := s.DBFS.Put("/f", []byte("z")); err != nil {
		t.Fatal(err)
	}
	if b, err := s.DBFS.Get("/f"); err != nil || string(b) != "z" {
		t.Fatal(err)
	}
	if err := s.DBFS.Mkdir("/dir"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DBFS.List("/"); err != nil {
		t.Fatal(err)
	}
	if err := s.DBFS.Move("/f", "/dir/f"); err != nil {
		t.Fatal(err)
	}
	if err := s.DBFS.Delete("/dir", true); err != nil {
		t.Fatal(err)
	}

	if err := s.Secrets.CreateScope("s"); err != nil {
		t.Fatal(err)
	}
	if err := s.Secrets.CreateScope("s"); err == nil {
		t.Fatal("dup scope")
	}
	if err := s.Secrets.Put("s", "k", "v"); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Secrets.Resolve("s", "k"); err != nil || v != "v" {
		t.Fatal(err)
	}
	if _, err := s.Secrets.Resolve("s", "missing"); err == nil {
		t.Fatal("missing secret")
	}
	if _, err := s.Secrets.ListKeys("s"); err != nil {
		t.Fatal(err)
	}
	_ = s.Secrets.ListScopes()
	if err := s.Secrets.DeleteKey("s", "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.Secrets.DeleteScope("s"); err != nil {
		t.Fatal(err)
	}

	job := s.Jobs.Create("j", []Task{{Key: "t"}})
	if _, ok := s.Jobs.Get(job.ID); !ok {
		t.Fatal("job")
	}
	_ = s.Jobs.List()
	run := s.Jobs.NewRun(job.ID)
	s.Jobs.UpdateRun(run)
	if _, ok := s.Jobs.GetRun(run.ID); !ok {
		t.Fatal("run")
	}
	_ = s.Jobs.ListRuns(job.ID)
	s.Jobs.CancelRun(run.ID)
	s.Jobs.Reset(job.ID, "j2", []Task{{Key: "u"}})
	s.Jobs.Delete(job.ID)
}

func TestSecretsPersistAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Secrets.CreateScope("kv"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Secrets.Put("kv", "pw", "s3cret"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Secrets.CreateAKVScope("akv", "/subscriptions/x/vaults/dev", "https://keyvault-emulator:4997"); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s2.Secrets.Resolve("kv", "pw")
	if err != nil || v != "s3cret" {
		t.Fatalf("persist resolve %q %v", v, err)
	}
	sc, ok := s2.Secrets.GetScope("akv")
	if !ok || sc.Backend != BackendAzureKeyVault || sc.DNSName == "" {
		t.Fatalf("akv scope %+v ok=%v", sc, ok)
	}
	if err := s2.Secrets.Put("akv", "pw", "nope"); err == nil {
		t.Fatal("put on AKV-backed scope succeeded")
	}
}

func TestOpenCorruptSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir+"/secrets", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/secrets/scopes.json", []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 1); err == nil {
		t.Fatal("corrupt secrets accepted")
	}
}

func TestIdentityCreateDelete(t *testing.T) {
	s, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	val, info, err := s.Identity.CreatePAT("bob", "c", 1)
	if err != nil || info.ID == "" {
		t.Fatal(err)
	}
	if _, ok := s.Identity.LookupPAT(val); !ok {
		t.Fatal("lookup")
	}
	if !s.Identity.DeletePAT(info.ID) || s.Identity.DeletePAT("nope") {
		t.Fatal("delete")
	}
	if _, ok := s.Identity.LookupClient(SeededClientID, s.OIDCSecret); !ok {
		t.Fatal("client")
	}
	if _, ok := s.Identity.LookupClient(SeededClientID, "no"); ok {
		t.Fatal("bad secret")
	}
	_ = s.Identity.ListPATs()
}

func TestOpenCorruptIdentity(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/identity.json", []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 1); err == nil {
		t.Fatal("corrupt identity accepted")
	}
}
