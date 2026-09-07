module unigate

go 1.26

require modernc.org/sqlite v1.29.2

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.47.0 // indirect
	modernc.org/libc v1.41.0 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.7.2 // indirect
)

replace (
	github.com/hashicorp/golang-lru/v2 => ./third_party/github.com/hashicorp/golang-lru/v2
	github.com/kballard/go-shellquote => ./third_party/github.com/kballard/go-shellquote
	github.com/klauspost/cpuid/v2 => ./third_party/github.com/klauspost/cpuid/v2
	github.com/mattn/go-sqlite3 => ./third_party/github.com/mattn/go-sqlite3
	golang.org/x/tools => ./third_party/golang.org/x/tools
	lukechampine.com/uint128 => ./third_party/lukechampine.com/uint128
	modernc.org/cc/v3 => ./third_party/modernc.org/cc/v3
	modernc.org/ccgo/v3 => ./third_party/modernc.org/ccgo/v3
	modernc.org/gc/v3 => ./third_party/modernc.org/gc/v3
	modernc.org/opt => ./third_party/modernc.org/opt
	modernc.org/strutil => ./third_party/modernc.org/strutil
	modernc.org/token => ./third_party/modernc.org/token
)