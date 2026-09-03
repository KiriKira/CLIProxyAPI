package acp

import "encoding/json"

// Wire protocol version spoken by this client. The ACP spec negotiates a
// single integer MAJOR version during initialize. Pinned to the snapshot
// this implementation was built against; re-validate before bumping.
const protocolVersion = 1

// JSON-RPC 2.0 envelope for the ACP stdio transport.
//
// ACP speaks newline-delimited JSON (NDJSON) over the child process
// stdout/stdin: one complete JSON-RPC message per line terminated by \n.
// There is no Content-Length framing (unlike LSP).
//
// All payload-carrying fields use json.RawMessage so request IDs and
// results round-trip byte-for-byte: decoding numbers into `any` would turn
// them into float64 and corrupt large integer IDs on echo, and Result must
// accept any JSON value (null, array, string, object), not just objects.
type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcErrorObject `json:"error,omitempty"`
}

// rpcErrorObject mirrors the JSON-RPC 2.0 error object. Code follows the
// standard ranges (-32700 parse error, -32600 invalid request, -32601
// method not found, -32602 invalid params, -32603 internal error) plus
// ACP-specific transport errors surfaced locally. Data accepts any JSON
// value so upstream diagnostics are never dropped by the transport.
type rpcErrorObject struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Standard JSON-RPC error codes reused when replying to agent-initiated
// requests this client does not support.
const (
	rpcParseErrorCode     = -32700
	rpcInvalidRequestCode = -32600
	rpcMethodNotFoundCode = -32601
	rpcInvalidParamsCode  = -32602
	rpcInternalErrorCode  = -32603
)

// idPresent reports whether a raw request ID counts as present. JSON null
// is explicitly absent per JSON-RPC 2.0, so an `id:null` payload must not
// be treated as a request or response identifier.
func idPresent(id json.RawMessage) bool {
	if len(id) == 0 {
		return false
	}
	return string(id) != "null"
}

// isResponse reports whether the message is a response to a previous
// request: it carries an ID and exactly one of result/error. A present
// `result:null` is a legal result, not an absent one.
func (m *wireMessage) isResponse() bool {
	if !idPresent(m.ID) {
		return false
	}
	hasResult := len(m.Result) > 0
	hasError := m.Error != nil
	return hasResult != hasError
}

// isRequest reports whether the message is a request expecting a response:
// it carries both a non-null ID and a method.
func (m *wireMessage) isRequest() bool {
	return idPresent(m.ID) && m.Method != ""
}

// isNotification reports whether the message is a one-way notification:
// a method with no (or null) ID.
func (m *wireMessage) isNotification() bool {
	return !idPresent(m.ID) && m.Method != ""
}

// InitializeRequest params for the ACP initialize handshake. The
// clientCapabilities shape mirrors the verified wire format: nested
// fs.readTextFile/fs.writeTextFile plus a top-level terminal flag.
// Explicit false means "not supported" and must be sent, not omitted.
type initializeRequest struct {
	ProtocolVersion    int                `json:"protocolVersion"`
	ClientCapabilities clientCapabilities `json:"clientCapabilities"`
	ClientInfo         clientInfo         `json:"clientInfo"`
}

type clientCapabilities struct {
	FS       fileSystemCapabilities `json:"fs"`
	Terminal bool                   `json:"terminal"`
}

type fileSystemCapabilities struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

type clientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// defaultClientCapabilities declares the Slice 1 safe posture: the proxy
// never grants the agent file-system or terminal access, matching the
// text-generation-only mode. Explicit false, not omitted.
func defaultClientCapabilities() clientCapabilities {
	return clientCapabilities{
		FS:       fileSystemCapabilities{ReadTextFile: false, WriteTextFile: false},
		Terminal: false,
	}
}

// InitializeResponse carries the agent-selected version plus its
// capabilities and supported auth methods. RawMessage fields preserve
// unknown capability shapes for forward compatibility.
type initializeResponse struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities json.RawMessage   `json:"agentCapabilities,omitempty"`
	AgentInfo         json.RawMessage   `json:"agentInfo,omitempty"`
	AuthMethods       []json.RawMessage `json:"authMethods,omitempty"`
}

// AuthenticateRequest params select one of the agent-advertised methods.
type authenticateRequest struct {
	MethodID string `json:"methodId"`
}

// NewSessionRequest params open a new agent session. MCPServers is always
// serialized (as [] when empty) because the ACP schema expects an array,
// not an absent field.
type newSessionRequest struct {
	CWD        string            `json:"cwd,omitempty"`
	MCPServers []json.RawMessage `json:"mcpServers"`
}

// NewSessionResponse carries the agent-assigned session identifier.
type newSessionResponse struct {
	SessionID string `json:"sessionId"`
}

// PromptRequest params deliver one conversational turn.
type promptRequest struct {
	SessionID string        `json:"sessionId"`
	Prompt    []promptBlock `json:"prompt"`
}

// promptBlock is a single ACP content block (text/resource) in a prompt.
type promptBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Resource json.RawMessage `json:"resource,omitempty"`
}

// PromptResponse is the terminal result of a prompt turn.
type promptResponse struct {
	StopReason string `json:"stopReason"`
}

// CancelNotification params interrupt the in-flight prompt turn.
// Cancel is a notification: no response is expected; completion is
// signaled by the prompt response carrying a cancelled stopReason.
type cancelNotification struct {
	SessionID string `json:"sessionId"`
}

// SessionUpdateParams is the params object of a session/update
// notification: the session ID plus a nested update object whose
// sessionUpdate field discriminates the event kind.
type sessionUpdateParams struct {
	SessionID string          `json:"sessionId"`
	Update    json.RawMessage `json:"update"`
}

// sessionUpdateEnvelope decodes just the discriminator of the nested
// update object. Known kinds: agent_message_chunk, agent_thought_chunk,
// tool_call, tool_call_update, plan, available_commands_update.
type sessionUpdateEnvelope struct {
	SessionUpdate string `json:"sessionUpdate"`
}

// SessionUpdate is the decoded client-facing streaming event: the session
// ID, the update kind discriminator, and the raw update object for
// downstream translation.
type SessionUpdate struct {
	SessionID string
	Kind      string
	Raw       json.RawMessage
}

// cancelledPermissionResult is the protocol-compliant denial returned for
// every session/request_permission while the proxy runs in the safe
// text-generation posture. Returning a valid outcome (rather than a
// method-not-found error) keeps the turn alive without granting access.
func cancelledPermissionResult() json.RawMessage {
	return json.RawMessage(`{"outcome":{"outcome":"cancelled"}}`)
}
