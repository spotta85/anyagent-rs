/* Generated from packages/schema.json by npm run types. Do not edit. */

/**
 * One line from the app: the id to reply to, and the command.
 */
export type Frame = {
  id: number;
} & Frame1;
export type Frame1 =
  | {
      cmd: "discover";
    }
  | {
      agent: AgentRef;
      cmd: "probe";
    }
  | {
      agent: AgentRef;
      cmd: "plan_usage";
    }
  | {
      agent: AgentRef;
      dir: string;
      prompt: string;
      resume?: string | null;
      fork?: string | null;
      fork_at?: string | null;
      permission_mode?: PermissionMode | null;
      mcp_servers?: McpServer[];
      configure?: {
        [k: string]: ConfigValue;
      };
      cmd: "generate";
    }
  | {
      agent: AgentRef;
      dir: string;
      resume?: string | null;
      fork?: string | null;
      fork_at?: string | null;
      permission_mode?: PermissionMode | null;
      mcp_servers?: McpServer[];
      configure?: {
        [k: string]: ConfigValue;
      };
      cmd: "open";
    }
  | {
      session: string;
      text: string;
      attachments?: string[];
      cmd: "prompt";
    }
  | {
      session: string;
      prompt: string;
      cmd: "dequeue";
    }
  | {
      session: string;
      request: string;
      answer: Answer;
      cmd: "answer";
    }
  | {
      session: string;
      option: string;
      value: ConfigValue;
      cmd: "configure";
    }
  | {
      session: string;
      turns: number;
      scope: RollbackScope;
      cmd: "rollback";
    }
  | {
      session: string;
      cmd: "compact";
    }
  | {
      session: string;
      clear_queue?: boolean;
      cmd: "cancel";
    }
  | {
      session: string;
      cmd: "info";
    }
  | {
      session: string;
      cmd: "close";
    };
/**
 * A catalog id like `"claude"`, or an ACP agent the catalog does not know.
 */
export type AgentRef =
  | string
  | {
      acp: AcpSpec;
    };
/**
 * How anyagent handles tool permission requests.
 */
export type PermissionMode = "Ask" | "AutoApprove";
export type McpConnection =
  | {
      Stdio: {
        command: string;
        args: string[];
        env: {
          [k: string]: string;
        };
      };
    }
  | {
      Http: {
        url: string;
        headers: {
          [k: string]: string;
        };
      };
    }
  | {
      Sse: {
        url: string;
        headers: {
          [k: string]: string;
        };
      };
    };
export type ConfigValue = string | boolean;
export type Answer =
  | {
      Permission: PermissionChoice;
    }
  | {
      Question: QuestionAnswer[];
    };
export type PermissionChoice = "AllowOnce" | "AllowAlways" | "DenyOnce" | "DenyAlways";
export type QuestionAnswer =
  | {
      Choices: string[];
    }
  | {
      Text: string;
    };
/**
 * What `rollback` rewinds: conversation context only, or also the files
 * the agent changed in the dropped turns (requires `RollbackFiles`).
 */
export type RollbackScope = "Conversation" | "ConversationAndFiles";
/**
 * One line to the app. Untagged, so each variant is a flat object.
 */
export type Line =
  | {
      hello: Hello;
    }
  | {
      id: number;
      ok: unknown;
    }
  | {
      id?: number | null;
      error: unknown;
    }
  | {
      event: Event;
    }
  | {
      session: string;
      error: unknown;
    }
  | {
      closed: string;
    };
export type EventKind =
  | {
      TurnStarted: {
        origin: TurnOrigin;
      };
    }
  | {
      TextDelta: {
        message_id: string;
        text: string;
      };
    }
  | {
      ReasoningDelta: {
        message_id: string;
        text: string;
      };
    }
  | {
      UserMessage: {
        message_id: string;
        text: string;
      };
    }
  | {
      MessageEnded: {
        message_id: string;
      };
    }
  | {
      ToolUpdated: ToolUpdate;
    }
  | {
      ToolOutputDelta: {
        tool_id: string;
        text: string;
      };
    }
  | {
      PlanUpdated: {
        entries: PlanEntry[];
      };
    }
  | {
      RequestOpened: Request;
    }
  | {
      RequestClosed: {
        request_id: string;
      };
    }
  | {
      SessionUpdated: SessionInfo;
    }
  | {
      StatusChanged: SessionStatus;
    }
  | {
      ContextUsage: {
        used_tokens: number;
        window_tokens?: number | null;
        cost_usd?: number | null;
      };
    }
  | "ContextCompacted"
  | {
      PlanUsageUpdated: PlanUsage;
    }
  | {
      Diagnostic: Diagnostic;
    }
  | {
      TurnEnded: {
        stop: StopReason;
        background: string[];
      };
    };
export type TurnOrigin =
  | "Agent"
  | {
      Prompt: string;
    };
export type ToolKind =
  | ("Read" | "Edit" | "Delete" | "Move" | "Search" | "Execute" | "Fetch" | "Think" | "Other")
  | {
      Mcp: {
        server: string;
        tool: string;
      };
    }
  | "Subagent";
export type ToolStatus = "Pending" | "Running" | "Completed" | "Failed" | "Cancelled";
export type ToolInput =
  | "None"
  | {
      Path: string;
    }
  | {
      Command: {
        command: string;
        cwd?: string | null;
      };
    }
  | {
      Pattern: string;
    }
  | {
      Url: string;
    }
  | {
      Query: string;
    }
  | {
      Text: string;
    };
export type PlanStatus = "Pending" | "InProgress" | "Completed";
/**
 * Something the agent is waiting on the caller for. Answer once with
 * `Session::answer`.
 */
export type Request =
  | {
      Permission: PermissionRequest;
    }
  | {
      Question: QuestionRequest;
    };
/**
 * Where discovery found an executable.
 */
export type InstallationSource =
  "EnvOverride" | "Path" | "LoginShellPath" | "VersionManager" | "KnownLocation" | "Pinned";
/**
 * Whether, and how, an agent is logged in.
 */
export type AuthStatus =
  | "Unknown"
  | {
      Authenticated: {
        kind: AuthKind;
        account?: AccountInfo | null;
      };
    }
  | {
      Unauthenticated: {
        login: LoginMethod[];
      };
    };
/**
 * The login kind decides which features exist (plan usage needs a subscription).
 */
export type AuthKind =
  | ("Subscription" | "ApiKey" | "CloudProvider")
  | {
      Other: string;
    };
/**
 * A login method the application can show to the user.
 */
export type LoginMethod =
  | {
      Terminal: {
        command: string[];
        env: {
          [k: string]: string;
        };
        description: string;
      };
    }
  | {
      EnvVar: {
        name: string;
      };
    };
/**
 * Optional actions supported by an agent or session.
 */
export type Capability =
  | (
      | "Images"
      | "Resume"
      | "Steer"
      | "Permissions"
      | "Questions"
      | "Rollback"
      | "Fork"
      | "SlashCommands"
      | "Plan"
      | "Subagents"
      | "ContextUsage"
      | "PlanUsage"
    )
  | "RollbackFiles"
  | "Compact";
export type McpTransport = "Stdio" | "Http" | "Sse";
export type ConfigKind =
  | "Boolean"
  | {
      Select: {
        choices: ConfigChoice[];
      };
    };
/**
 * What a UI should show for the session right now. Changes arrive as
 * `EventKind::StatusChanged`; `Session::status` reads it without the stream.
 */
export type SessionStatus = "Idle" | "Working" | "NeedsInput";
export type DiagnosticLevel = "Info" | "Warning" | "Error";
export type StopReason =
  | ("Cancelled" | "Refused")
  | {
      Completed: {
        source: CompletionSource;
      };
    }
  | {
      Failed: {
        message: string;
      };
    };
export type CompletionSource = "Protocol" | "Inferred";
export type DeliveryKind =
  | {
      Started: {
        turn_id: string;
      };
    }
  | {
      Steered: {
        turn_id: string;
      };
    }
  | {
      Queued: {
        /**
         * Number of prompts ahead of this one.
         */
        position: number;
      };
    };

/**
 * The wire's entry points; each field puts one type under `definitions`.
 */
export interface Protocol {
  command: Frame;
  line: Line;
  event: Event;
  error: ErrorBody;
  discovery: DiscoveryReport;
  details: AgentDetails;
  plan_usage: PlanUsage;
  session_info: SessionInfo;
  delivery: Delivery;
}
export interface AcpSpec {
  name: string;
  path: string;
  args?: string[];
}
/**
 * A client-owned MCP server the agent should connect to, forwarded at open.
 */
export interface McpServer {
  name: string;
  connection: McpConnection;
}
/**
 * The first line: which protocol, from which crate version.
 */
export interface Hello {
  protocol: number;
  anyagent: string;
}
/**
 * One normalized event produced by anyagent.
 */
export interface Event {
  /**
   * Starts at 1 and increases for every session event.
   */
  sequence: number;
  /**
   * When the engine produced this event — wall-clock, so transcripts can
   * be stored and merged without the app stamping receive time. Order
   * across events is guaranteed by `sequence`, not by this clock. A
   * transcript stored before this field existed reads as the epoch.
   */
  occurred_at?: SystemTime;
  session_id: string;
  /**
   * Present when the event belongs to a turn.
   */
  turn_info?: TurnContext | null;
  kind: EventKind;
  /**
   * Provider-specific data that has not been normalized.
   */
  extensions: {
    [k: string]: unknown;
  };
}
export interface SystemTime {
  secs_since_epoch: number;
  nanos_since_epoch: number;
}
export interface TurnContext {
  id: string;
  /**
   * Present for events produced by a subagent spawned from this tool call.
   */
  parent_tool_id?: string | null;
}
/**
 * Cumulative snapshot of one tool call.
 */
export interface ToolUpdate {
  id: string;
  kind: ToolKind;
  title: string;
  status: ToolStatus;
  input: ToolInput;
  output?: string | null;
  diffs: FileDiff[];
  locations: string[];
  /**
   * Agent's own tool name and raw input, for unknown or MCP tools.
   */
  raw?: RawTool | null;
}
export interface FileDiff {
  path: string;
  /**
   * `None` means a new file.
   */
  old_text?: string | null;
  new_text: string;
}
export interface RawTool {
  name: string;
  input: unknown;
}
export interface PlanEntry {
  text: string;
  status: PlanStatus;
}
export interface PermissionRequest {
  id: string;
  /**
   * The tool call awaiting approval, as the app already saw it.
   */
  tool: ToolUpdate;
  /**
   * What the agent offers; answers outside this list are rejected.
   */
  options: PermissionChoice[];
  detail?: string | null;
}
export interface QuestionRequest {
  id: string;
  questions: Question[];
}
export interface Question {
  id: string;
  text: string;
  header?: string | null;
  /**
   * Empty means free text only.
   */
  choices: Choice[];
  multi_select: boolean;
  allows_free_text: boolean;
}
export interface Choice {
  id: string;
  label: string;
  description?: string | null;
}
/**
 * Snapshot of a live session. Also carried by `EventKind::SessionUpdated`.
 */
export interface SessionInfo {
  id: string;
  agent: AgentInstallation;
  details: AgentDetails;
  configuration: SessionConfiguration;
  resume_token?: string | null;
  /**
   * Agent-suggested title when the wire provides one.
   */
  title?: string | null;
  /**
   * Current UI state; kept fresh by the engine.
   */
  status?: SessionStatus & string;
}
/**
 * One installed agent, as returned by `Runtime::discover`.
 */
export interface AgentInstallation {
  id: string;
  name: string;
  executable_path: string;
  source: InstallationSource;
  /**
   * A richer runtime for this agent that is not installed (Antigravity's
   * ACP server). `open` works without it with fewer capabilities; the
   * record says what to install to get the rest.
   */
  upgrade?: MissingAgent | null;
  acp_args?: string[] | null;
}
export interface MissingAgent {
  id: string;
  name: string;
  searched: string[];
  install_hint: string;
}
/**
 * What `probe` and `open` learn about an agent.
 */
export interface AgentDetails {
  version?: string | null;
  auth: AuthStatus;
  capabilities: Capabilities;
  config_options: ConfigOption[];
  commands: SlashCommand[];
}
export interface AccountInfo {
  email?: string | null;
  plan?: string | null;
}
/**
 * Effective caller actions for one agent or session.
 */
export interface Capabilities {
  features: Capability[];
  mcp_transports: McpTransport[];
}
/**
 * A session setting the agent advertises. Well-known ids: `model`, `effort`,
 * `mode`, `sandbox`, `fast` (boolean, lower latency with increased usage).
 */
export interface ConfigOption {
  id: string;
  name: string;
  category?: string | null;
  kind: ConfigKind;
  current?: ConfigValue | null;
  /**
   * Changeable through `Session::configure`; the adapter may resume internally.
   */
  live: boolean;
}
export interface ConfigChoice {
  value: string;
  label: string;
  description?: string | null;
}
export interface SlashCommand {
  name: string;
  description: string;
  input_hint?: string | null;
}
export interface SessionConfiguration {
  options: {
    [k: string]: ConfigValue;
  };
}
/**
 * Plan quota windows for the logged-in account.
 */
export interface PlanUsage {
  /**
   * The plan the quota belongs to ("max", "edu"), when the agent names it.
   */
  plan?: string | null;
  windows: UsageWindow[];
  fetched_at: SystemTime;
}
export interface UsageWindow {
  /**
   * "Session" (5h), "Week", or an agent-provided label.
   */
  label: string;
  used_percent: number;
  resets_at?: SystemTime | null;
}
export interface Diagnostic {
  level: DiagnosticLevel;
  message: string;
}
/**
 * The `error` object: `kind`, `message`, and the variant's own fields.
 */
export interface ErrorBody {
  kind: string;
  message: string;
  [k: string]: unknown;
}
/**
 * What `discover` found and what it could not read.
 */
export interface DiscoveryReport {
  agents: AgentInstallation[];
  /**
   * Known agents that were not found, with where we looked and how to
   * install them.
   */
  missing: MissingAgent[];
  diagnostics: Diagnostic[];
}
/**
 * Immediate result of submitting a prompt.
 */
export interface Delivery {
  /**
   * Stable across immediate delivery and later queue promotion.
   */
  prompt_id: string;
  kind: DeliveryKind;
}
