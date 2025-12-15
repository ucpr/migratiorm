## migratiorm

>[!WARNING]
> This project is currently in an early development stage. Features and APIs may change significantly in future releases.

## Overview

migratiorm is a test helper package that supports ORM migration projects in Go.
It makes it easy to verify that no regressions are introduced during ORM migrations.

## Features

TBD

## Usage

To install migratiorm, use the following command:

```
go get github.com/ucpr/migratiorm
```

```
import (
	"database/sql"
	"testing"

	"github.com/ucpr/migratiorm"
)

func TestMigratiorm_BasicUsage(t *testing.T) {
	m := migratiorm.New()

	m.Expect(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Actual(func(db *sql.DB) {
		db.Query("SELECT * FROM users WHERE age > ?", 18)
	})

	m.Assert(t)
}
```

## Contributing

Contributions are welcome! Please open issues and submit pull requests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
