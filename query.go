package migratiorm

// OperationType represents the type of SQL operation.
type OperationType int

const (
	OperationSelect OperationType = iota
	OperationInsert
	OperationUpdate
	OperationDelete
	OperationOther
)

func (o OperationType) String() string {
	switch o {
	case OperationSelect:
		return "SELECT"
	case OperationInsert:
		return "INSERT"
	case OperationUpdate:
		return "UPDATE"
	case OperationDelete:
		return "DELETE"
	default:
		return "OTHER"
	}
}

// Query represents a captured SQL query.
type Query struct {
	Raw        string        // Original query before normalization
	Normalized string        // Query after normalization
	Args       []any         // Bind parameters
	Operation  OperationType // Type of operation (SELECT, INSERT, etc.)
}

// detectOperation detects the operation type from a SQL query.
func detectOperation(query string) OperationType {
	if len(query) < 6 {
		return OperationOther
	}

	// Normalize first characters to uppercase for comparison
	prefix := make([]byte, 6)
	for i := 0; i < 6 && i < len(query); i++ {
		c := query[i]
		if c >= 'a' && c <= 'z' {
			c -= 32 // Convert to uppercase
		}
		prefix[i] = c
	}

	switch string(prefix) {
	case "SELECT":
		return OperationSelect
	case "INSERT":
		return OperationInsert
	case "UPDATE":
		return OperationUpdate
	case "DELETE":
		return OperationDelete
	default:
		return OperationOther
	}
}
