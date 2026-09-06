package security

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// wholeTextScan evaluates every pattern over the whole text, the behaviour
// before the keyword index existed; the windowed evaluation must agree with
// it on every input.
func wholeTextScan(text string) *secretScan {
	scan := newSecretScan(text)
	scan.full = true
	return scan
}

func assertWindowedScanMatchesWholeText(t *testing.T, text string) {
	t.Helper()
	stripped := stripUnsafeTextRunes(text)
	windowed := looksLikeSecret(newSecretScan(stripped))
	whole := looksLikeSecret(wholeTextScan(stripped))
	if windowed != whole {
		t.Fatalf("LooksLikeSecret(%q): windowed = %v, whole-text = %v", text, windowed, whole)
	}
}

func TestFoldUnsafeRunesAreComplete(t *testing.T) {
	t.Parallel()
	var want []rune
	for r := rune(0x80); r <= unicode.MaxRune; r++ {
		unsafe := unicode.ToLower(r) < 0x80 || unicode.ToUpper(r) < 0x80
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < 0x80 {
				unsafe = true
			}
		}
		if unsafe {
			want = append(want, r)
		}
	}
	if got := []rune(foldUnsafeRunes); string(got) != string(want) {
		t.Fatalf("foldUnsafeRunes = %U, Unicode tables require %U", got, want)
	}
	for _, r := range want {
		if !newSecretScan("x" + string(r) + "x").full {
			t.Fatalf("text containing %U must disable keyword windows", r)
		}
	}
	if newSecretScan("plain ascii and ünïcödé").full {
		t.Fatal("text without fold-unsafe runes must keep keyword windows")
	}
}

func TestSecretScanWindowsAreLineAlignedAndMerged(t *testing.T) {
	t.Parallel()
	text := "intro\n  password =\n\n    \"value\"\nmiddle\nno match here\nAuthorization: Bearer abc\nlast"
	scan := newSecretScan(text)
	spans := scan.windows(credentialKeywords, secretScanTokenRuns)
	if len(spans) != 1 {
		t.Fatalf("windows = %+v, want one span", spans)
	}
	if got := text[spans[0].start:spans[0].end]; got != "  password =\n\n    \"value\"\nmiddle\nno match here" {
		t.Fatalf("window = %q", got)
	}
	// The single-line prefix and JWT patterns get exactly the keyword's line.
	spans = scan.windows(authorizationKeywords, 0)
	if len(spans) != 1 || text[spans[0].start:spans[0].end] != "Authorization: Bearer abc" {
		t.Fatalf("zero-run window = %+v", spans)
	}
	// Adjacent keyword lines merge into one contiguous window.
	scan = newSecretScan("token: a\nsecret: b\n\n\nunrelated\npassword: c")
	spans = scan.windows(credentialKeywords, 0)
	if len(spans) != 2 || spans[0] != (textSpan{start: 0, end: 18}) || spans[1] != (textSpan{start: 31, end: 42}) {
		t.Fatalf("merged windows = %+v", spans)
	}
	if lines := scan.credentialKeyLines(); fmt.Sprint(lines) != "[0 1 5]" {
		t.Fatalf("credential key lines = %v", lines)
	}
	if !newSecretScan("").full && len(newSecretScan("").windows(credentialKeywords, 4)) != 0 {
		t.Fatal("empty text produced windows")
	}
}

func TestSecretScanDenseKeywordsFallBackBeforeIndexGrows(t *testing.T) {
	t.Parallel()
	text := strings.Repeat("token ", secretScanMaxKeywordOccurrences+1)
	scan := newSecretScan(text)
	spans := scan.windows(credentialKeywords, secretScanTokenRuns)
	if !scan.full {
		t.Fatal("dense keyword input did not fall back to whole-text evaluation")
	}
	if len(spans) != 1 || spans[0] != (textSpan{start: 0, end: len(text)}) {
		t.Fatalf("dense keyword spans = %+v, want the whole text", spans)
	}
	assertWindowedScanMatchesWholeText(t, text)
}

func TestLooksLikeSecretWindowedMatchesWholeTextOnKnownCases(t *testing.T) {
	t.Parallel()
	for _, text := range secretScanKnownCases() {
		assertWindowedScanMatchesWholeText(t, text)
		// Every known case embedded in unrelated text, with keyword-free
		// lines before and after and the text indented and re-cased.
		assertWindowedScanMatchesWholeText(t, "plain line\n"+text+"\ntrailing line\n")
		assertWindowedScanMatchesWholeText(t, "  "+strings.ReplaceAll(text, "\n", "\n  "))
		assertWindowedScanMatchesWholeText(t, strings.ToUpper(text))
		assertWindowedScanMatchesWholeText(t, "- \n"+text)
		assertWindowedScanMatchesWholeText(t, text+"\n"+text)
	}
}

func TestLooksLikeSecretWindowedMatchesWholeTextOnGeneratedDocuments(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(462))
	for range 20000 {
		assertWindowedScanMatchesWholeText(t, generateSecretScanDocument(rng))
	}
}

// TestLooksLikeSecretWindowedMatchesWholeTextOnCorpus checks every regular
// file under ORKA_SECRET_SCAN_CORPUS_DIR (a repository checkout, for example)
// when the variable is set.
func TestLooksLikeSecretWindowedMatchesWholeTextOnCorpus(t *testing.T) {
	files := loadSecretScanCorpus(t)
	for _, file := range files {
		assertWindowedScanMatchesWholeText(t, string(file.content))
		stripped := stripUnsafeTextRunes(string(file.content))
		for line := range strings.SplitSeq(stripped, "\n") {
			assertWindowedScanMatchesWholeText(t, line)
		}
	}
}

func FuzzLooksLikeSecretWindowedMatchesWholeText(f *testing.F) {
	for _, text := range secretScanKnownCases() {
		f.Add(text)
	}
	rng := rand.New(rand.NewSource(1))
	for range 64 {
		f.Add(generateSecretScanDocument(rng))
	}
	f.Fuzz(func(t *testing.T, text string) {
		assertWindowedScanMatchesWholeText(t, text)
	})
}

func secretScanKnownCases() []string {
	cases := append([]string(nil), looksLikeSecretNegatives...)
	cases = append(cases, looksLikeSecretPositives...)
	cases = append(cases,
		"",
		"\n\n\n",
		"password",
		"password:",
		"password =",
		"credentials\n\n=\n\n"+strings.Repeat("v", 20),
		"credentials\n  =\n  "+strings.Repeat("v", 20),
		"\"password\"\n\n:  "+strings.Repeat("v", 20),
		"Authorization\n:\nBearer\n"+strings.Repeat("q", 24),
		"Cookie\n:\nsessionid="+strings.Repeat("q", 24),
		"Txn-Token\n:\n"+strings.Repeat("t", 32),
		"ey"+"J"+strings.Repeat("a", 12)+"."+strings.Repeat("b", 12)+"."+strings.Repeat("c", 12),
		"x"+"ey"+"J"+strings.Repeat("a", 12)+"."+strings.Repeat("b", 12)+"."+strings.Repeat("c", 12),
		"AK"+"IA"+strings.Repeat("A1", 8),
		"password = load(\n  ctx,\n)\n  + \""+strings.Repeat("stapl", 5)+"\"",
		"password = load(ctx)\n\n\n\n\n\n  + \""+strings.Repeat("stapl", 5)+"\"",
		"password = load(ctx)\n// a\n// b\n# c\n/* d */\n  + \""+strings.Repeat("stapl", 5)+"\"",
		"password: \"open quote\n  never closes",
		"-\n  password: "+strings.Repeat("p", 20),
		"- \n\n\n  \"password\": "+strings.Repeat("p", 20),
		"passwordİ="+strings.Repeat("p", 20),
		"paſſword="+strings.Repeat("p", 20),
		"to"+"K"+"en="+strings.Repeat("p", 20),
		"tokenı: "+strings.Repeat("p", 20),
		"-----BEGİN "+"RSA PRIVATE KEY-----",
		"-----BEGIN\n"+"RSA PRIVATE KEY-----",
		"PASSWORD\u200b="+strings.Repeat("p", 20),
		"pass\u200bword="+strings.Repeat("p", 20),
		"password=\r\n"+strings.Repeat("p", 20),
		"password=\x00"+strings.Repeat("p", 20),
		"password=\xff"+strings.Repeat("p", 20),
		"?sig="+strings.Repeat("Zx", 12),
		"&x-goog-credential="+strings.Repeat("Zx", 12),
		"a?token="+strings.Repeat("Zx", 12),
	)
	return cases
}

var secretScanKeywords = []string{
	"password", "PASSWORD", "Password", "api_key", "apiKey", "API-KEY", "token", "access_token",
	"ID_TOKEN", "secret", "client_secret", "credentials", "credential", "private_key", "privateKey",
	"OPENAI_API_KEY", "x-secret", "my-password", "authorization", "Authorization", "Cookie",
	"Set-Cookie", "Txn-Token", "txn-token", "tx-token", "passwd", "key", "user",
}

var secretScanValues = []string{
	strings.Repeat("v", 16), strings.Repeat("v", 15), "correct-horse-battery-staple",
	"\"correct horse battery staple\"", "'pass phrase with spaces!'", "$TOKEN", "${TOKEN}", "{{ .Token }}",
	"<your-token>", "[REDACTED]", "%PASSWORD%", "changeme", "dummy", "placeholder", "short",
	"strings.TrimSpace(cfg.APIKey)", "cfg.Provider.APIKey", "readPasswordFromKeychain(ctx)",
	"os.Getenv(\"X\")", "load(", "normalize(\n  input,\n)", "config.production.password",
	"correct.horse.battery.staple", "p@ssword-correct-horse", "${UNSET:-correct-horse-battery-staple}",
	"\"abcdefgh\n  ijklmnop\"", "\"correct-\\\n  horse-battery-staple\"", "|", "|-", ">-", ">", "|2-",
	"", "#", "# comment", "Bearer " + strings.Repeat("q", 24), "Basic dXNlcjpwYXNzd29yZA==",
	"Bearer $TOKEN", "bearer eyJ" + strings.Repeat("a", 12) + "." + strings.Repeat("b", 12) + "." + strings.Repeat("c", 12),
	"sessionid=" + strings.Repeat("q", 24) + "; HttpOnly", "theme=dark", "sk-" + strings.Repeat("a", 24),
	"ghp_" + strings.Repeat("x", 36), "AKIA" + strings.Repeat("A1", 8), "xoxb-" + strings.Repeat("1", 12),
	"https://h/p?sig=" + strings.Repeat("Zx", 12), "https://h/p?X-Amz-Signature=" + strings.Repeat("f0", 16),
	"-----BEGIN RSA PRIVATE KEY-----", "\"" + strings.Repeat("\\\n", 3) + strings.Repeat("live", 6) + "\"",
	"'quoted key': alice", "- name: alpha", "user: alice", "abcdefgh\n\n  ijklmnop",
}

var secretScanSeparators = []string{"=", ":", ": ", " = ", " : ", " :", ":=", "\n=", "\n:", " =\n", ":\n", " =\n\n", "\":", "\" : ", "':"}

var secretScanFiller = []string{
	"", "\n", "\n\n", "  ", "\t", "- ", "\"", "'", "// note\n", "# note\n", "/* note */", "/* open\n", "*/",
	"if x {\n", "}\n", "  + ", "\n  + ", "\n  or ", "\n  .concat(", "(", ")", ",", ";", "func f() {\n",
	"plain prose without keywords\n", "ünïcödé\n", "İ", "ı", "ſ", "K", "\u200b", "\r\n", "\x00", "\xff",
	"return apiKey\n", "other: value\n", "  user: alice\n", "  host: db\n", "  \"quoted key\": alice\n",
	"  # explanatory note\n", "  x\n", "end\"", "\"", "\n  ", "\n    ", "\n\t",
}

func generateSecretScanDocument(rng *rand.Rand) string {
	var b strings.Builder
	parts := 1 + rng.Intn(6)
	for range parts {
		switch rng.Intn(10) {
		case 0, 1, 2:
			b.WriteString(secretScanFiller[rng.Intn(len(secretScanFiller))])
		case 3:
			b.WriteString(secretScanKeywords[rng.Intn(len(secretScanKeywords))])
		case 4:
			b.WriteString(secretScanValues[rng.Intn(len(secretScanValues))])
		default:
			b.WriteString(secretScanFiller[rng.Intn(len(secretScanFiller))])
			b.WriteString(secretScanKeywords[rng.Intn(len(secretScanKeywords))])
			b.WriteString(secretScanSeparators[rng.Intn(len(secretScanSeparators))])
			b.WriteString(secretScanValues[rng.Intn(len(secretScanValues))])
			b.WriteString(secretScanFiller[rng.Intn(len(secretScanFiller))])
		}
	}
	text := b.String()
	if rng.Intn(8) == 0 {
		text = strings.ToUpper(text)
	}
	return text
}

type secretScanCorpusFile struct {
	path    string
	content []byte
}

func loadSecretScanCorpus(tb testing.TB) []secretScanCorpusFile {
	tb.Helper()
	dir := os.Getenv("ORKA_SECRET_SCAN_CORPUS_DIR")
	if dir == "" {
		tb.Skip("ORKA_SECRET_SCAN_CORPUS_DIR unset")
	}
	var files []secretScanCorpusFile
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files = append(files, secretScanCorpusFile{path: p, content: content})
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return files
}

// BenchmarkLooksLikeSecretCorpus measures the flagger over every file under
// ORKA_SECRET_SCAN_CORPUS_DIR, the pass the workspace baseline capture runs.
func BenchmarkLooksLikeSecretCorpus(b *testing.B) {
	files := loadSecretScanCorpus(b)
	var total int64
	for _, f := range files {
		total += int64(len(f.content))
	}
	b.SetBytes(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		flagged := 0
		for _, f := range files {
			if LooksLikeSecret(string(f.content)) {
				flagged++
			}
		}
		b.ReportMetric(float64(flagged), "flagged")
	}
}

// BenchmarkSecretLikeLineDigestsCorpus measures the fingerprinter over the
// corpus files the flagger marks, as the baseline capture does.
func BenchmarkSecretLikeLineDigestsCorpus(b *testing.B) {
	files := loadSecretScanCorpus(b)
	var flagged []string
	var total int64
	for _, f := range files {
		if LooksLikeSecret(string(f.content)) {
			flagged = append(flagged, string(f.content))
			total += int64(len(f.content))
		}
	}
	b.SetBytes(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, text := range flagged {
			SecretLikeLineDigests(text)
		}
	}
}
