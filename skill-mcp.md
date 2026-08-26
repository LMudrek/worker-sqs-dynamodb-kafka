MCP Tool Modeling — Agent-First Skill

Purpose

Design MCP tools as agent-facing contracts, not as thin wrappers around APIs.

A high-quality MCP tool must be:

* discoverable by an LLM;
* semantically self-contained;
* explicit about when it should and should not be used;
* strict enough to prevent invalid calls;
* concise enough to avoid unnecessary context consumption;
* stable even when the underlying API evolves;
* testable through realistic natural-language evaluation cases.

This skill applies generically to MCP tools, especially tools backed by REST, GraphQL, or internal APIs.

⸻

1. Core principle

Design the MCP contract for the agent, then adapt it to the API.

Do not start with:

API endpoint → copy OpenAPI schema → expose as MCP tool

Start with:

User intent
    ↓
LLM interpretation
    ↓
MCP semantic contract
    ↓
Validation
    ↓
Adapter / mapping
    ↓
API contract

The MCP contract is allowed to be different from the API contract when that improves agent reliability.

However, do not introduce unnecessary mappings. If an API value is already stable, meaningful, and appropriate for an agent, reuse it.

⸻

2. Tool boundary

DO

Create a tool around a meaningful agent task, not merely around an API endpoint.

A tool should have:

* one clear responsibility;
* a distinct purpose;
* a predictable input contract;
* a useful output contract;
* a clear relationship to neighboring tools.

Prefer:

search_financial_commitments

when the agent needs to find relevant commitments.

Avoid exposing many tiny tools that force the agent to manually reproduce a workflow that the server can perform deterministically.

DON’T

Do not blindly expose every API endpoint as a tool.

Do not create overlapping tools such as:

get_commitment
list_commitments
search_commitments
find_commitments
query_commitments

unless their semantics and use cases are genuinely distinct.

Do not make the agent perform deterministic business logic that the server can safely perform.

⸻

3. Tool name

DO

Use a short, explicit, action-oriented name.

Good:

list_financial_commitments
search_financial_commitments
get_payment_details

The name should help the model distinguish the tool from other tools.

DON’T

Avoid:

process
execute
handler
api_call
financial_api
tool1
getData

Avoid names whose meaning depends on internal implementation terminology.

⸻

4. Tool title

Use title as the human-readable label for the tool.

Good:

{
  "name": "list_financial_commitments",
  "title": "Listar compromissos financeiros"
}

Keep it short.

Do not use title as a replacement for detailed behavioral instructions.

⸻

5. Tool description

The tool description is one of the most important parts of the agent-facing contract.

Write it as if explaining the tool to a highly capable new engineer who does not know the internal system.

The description should answer:

1. What does the tool do?
2. When should the agent use it?
3. When should the agent NOT use it?
4. What concepts does it operate on?
5. What are the important business semantics?
6. What are important limitations?
7. What does each major filter mean?
8. What should the agent avoid inferring?

Good pattern:

Lists a customer's financial commitments, including scheduled and overdue
payments. Supports filtering by due-date range, commitment status, and
payment type.
Use when the user asks about their payments, bills, charges, or financial
commitments.
Do not infer a status or payment type when the user has not provided enough
information to determine one.

DO

Prefer precise behavioral instructions over marketing language.

DON’T

Do not write vague descriptions such as:

Gets financial data.

Do not assume the model knows internal terminology.

⸻

6. Input schema

Use JSON Schema to make the tool contract strict.

The MCP tool input schema should be designed to communicate both:

* machine-validatable constraints;
* semantic information useful to the LLM.

For current MCP tool schemas, JSON Schema 2020-12 is the relevant direction for the new specification generation.

Prefer:

{
  "type": "object",
  "additionalProperties": false,
  "properties": {}
}

when the tool has a closed, known set of inputs.

DO

Use:

* type;
* title;
* description;
* format;
* required;
* additionalProperties;
* oneOf;
* anyOf;
* allOf;
* $defs;
* $ref;
* numeric/string/array constraints;
* conditional schemas when they genuinely improve correctness.

DON’T

Do not use schema complexity merely because it is available.

Every constraint should prevent a realistic error or communicate meaningful semantics.

⸻

7. Required vs optional parameters

A parameter should be required only when the tool cannot correctly execute without it.

For optional filters:

{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "status": {}
  }
}

Do not make optional filters required just because the underlying API requires a value.

Conversely, do not hide a genuinely required business input behind an arbitrary default.

⸻

8. Parameter naming

Parameter names must describe the business meaning, not the implementation.

Good:

startDate
endDate
dueDate
paymentType
status
customerId

Better when ambiguity exists:

dueDateStart
dueDateEnd

Avoid:

start
end
type
value
data
user

Prefer:

customerId

over:

user

when the value is specifically a customer identifier.

⸻

9. Parameter title vs description

Use the two fields for different purposes.

title

Short label.

"title": "Tipo de pagamento"

description

Semantic and behavioral instructions.

"description": "Modalidade do compromisso financeiro. Use somente quando..."

Rule:

Title names the concept. Description explains the concept and how the agent should use it.

Do not put paragraphs into titles.

Do not rely on titles to communicate business rules.

⸻

10. Enums

Enums are one of the most important areas of agent-facing schema design.

10.1 First decide whether the API enum should be exposed

Ask:

Is the API value meaningful and stable enough for the LLM-facing contract?

If YES

Reuse it.

Example:

PIX
DDA
AUTOMATIC_BOLETO

can remain the MCP values if those values are stable and understandable.

If NO

Create a semantic MCP value and map it internally.

Example:

MCP:
PIX
DDA
AUTOMATIC_BOLETO
API:
01
03
07

with:

PIX               → 01
DDA               → 03
AUTOMATIC_BOLETO  → 07

This adapter protects the agent-facing contract from technical API codes.

DON’T

Do not create a mapping solely for the sake of abstraction.

Avoid:

DDA → DEBITO_DIRETO_AUTORIZADO → DDA

when the API already uses DDA and the value is acceptable.

⸻

11. Titled enums

When enum values need human-readable semantic labels, prefer the MCP/JSON Schema pattern:

{
  "type": "string",
  "title": "Tipo de pagamento",
  "description": "Modalidade do compromisso financeiro.",
  "oneOf": [
    {
      "const": "PIX",
      "title": "Pix"
    },
    {
      "const": "DDA",
      "title": "DDA"
    },
    {
      "const": "AUTOMATIC_BOLETO",
      "title": "Boleto automático"
    }
  ]
}

Use:

const       = canonical machine value
title       = concise label
description = meaning + selection guidance

Avoid relying on the legacy/non-standard enumNames approach when titled enum semantics are required.

⸻

12. Enum descriptions

If an enum has business meaning that is not obvious from its name, document that meaning.

Example:

{
  "const": "DDA",
  "title": "DDA",
  "description": "Débito Direto Autorizado: cobranças apresentadas ao cliente para consulta e eventual autorização."
}

This is especially important for:

* banking terms;
* internal statuses;
* abbreviations;
* product-specific concepts;
* regulatory concepts;
* lifecycle states.

DON’T

Do not assume that the LLM knows internal acronyms.

Bad:

{
  "const": "DDA",
  "title": "DDA"
}

when DDA is important to correctly interpret user intent.

⸻

13. Enum selection rules

For every important enum, answer:

1. What does the value mean?
2. What user language usually maps to it?
3. What user language must NOT map to it?
4. What distinguishes it from other enum values?
5. What should happen when the user is ambiguous?

Example:

expired:
The commitment passed its due date and is overdue.
too_expired:
The commitment exceeded the product's defined maximum overdue period.
scheduled:
The commitment has not reached its due date.

If the distinction depends on a concrete business rule, state the rule.

Do not expect the model to infer business boundaries.

⸻

14. Do not force the user to know enum names

The user may say:

"Quais cobranças chegaram para eu autorizar?"

The user does not need to say:

"DDA"

The tool schema should provide enough semantic context for the model to map natural language to:

{
  "paymentType": "DDA"
}

However, do not make aggressive mappings when the intent is ambiguous.

⸻

15. Do not invent filters

A filter should be populated only when there is enough evidence.

Example:

User:

Quais são meus pagamentos?

Correct:

{}

Not:

{
  "paymentType": "PIX"
}

User:

Quais pagamentos via Pix eu tenho?

Correct:

{
  "paymentType": "PIX"
}

Rule:

Never use an enum merely because it is available.

⸻

16. Natural-language examples

Examples are useful when they clarify mappings that are otherwise difficult for an LLM to infer.

Use examples especially for:

* ambiguous enums;
* dates;
* relative time;
* domain-specific terminology;
* complex combinations of filters.

Example:

"Pagamentos via Pix desta semana"
→ paymentType = PIX
"Contas atrasadas"
→ status = expired

Examples should be:

* realistic;
* concise;
* representative;
* focused on difficult cases.

DON’T

Do not fill the tool description with dozens of examples.

Use evaluation suites for broad coverage.

⸻

17. Dates and temporal semantics

A field named startDate is often ambiguous.

Explicitly define what date it represents.

Prefer:

{
  "type": "string",
  "format": "date",
  "title": "Data inicial do vencimento",
  "description": "Data inicial do período de consulta, inclusive. Refere-se à data de vencimento do compromisso. Formato YYYY-MM-DD."
}

rather than:

{
  "type": "string",
  "format": "date",
  "description": "Start date."
}

Clarify:

* timezone;
* inclusivity/exclusivity;
* business date vs timestamp;
* due date vs creation date vs payment date;
* relative date interpretation when relevant.

⸻

18. Defaults

Do not add defaults simply because the API has defaults.

A default changes agent behavior.

Before adding a default, ask:

1. Is the default semantically safe?
2. Would a user reasonably expect it?
3. Could it cause the tool to return misleading data?
4. Does it hide missing user intent?

Avoid defaults for filters when absence has meaningful semantics.

⸻

19. additionalProperties

For closed tool contracts, prefer:

"additionalProperties": false

This prevents the agent from inventing unsupported parameters.

Do not accept arbitrary fields merely to make the API proxy more flexible.

⸻

20. Output design

A tool is not well designed if only its input schema is good.

Return information optimized for the agent’s next step.

Prefer:

high-signal business fields

over:

internal implementation metadata

Avoid returning unnecessary:

* UUIDs;
* internal database keys;
* transport metadata;
* raw API envelopes;
* duplicated fields;
* implementation-specific details.

If technical identifiers are needed for subsequent tool calls, expose them intentionally and document their purpose.

⸻

21. Token efficiency

LLM context is a resource.

Optimize both:

tool description size
+
tool response size

Use:

* filtering;
* pagination;
* sensible limits;
* concise output;
* targeted searches;
* truncation where appropriate.

Do not return huge datasets when the agent only needs a small subset.

⸻

22. Error responses

Tool errors should be actionable.

Bad:

Error 400

Better:

Invalid date range: endDate must be greater than or equal to startDate.
Use YYYY-MM-DD.

An agent should be able to recover from the error without external documentation whenever possible.

Do not expose raw stack traces or internal infrastructure details.

⸻

23. API adapter strategy

Treat the MCP contract and API contract as separate boundaries.

Recommended:

MCP semantic contract
        ↓
MCP application/service layer
        ↓
API adapter
        ↓
External/internal API

The adapter may:

* rename fields;
* map enums;
* convert dates;
* normalize values;
* combine API calls;
* split one MCP request into multiple API requests;
* normalize API responses.

DO

Keep the MCP contract stable when API implementation details change.

DON’T

Leak API quirks into the agent-facing schema unless they are genuinely meaningful to the user task.

⸻

24. Security and authorization

Never assume that a valid schema means an authorized operation.

Validate:

* identity;
* authorization;
* tenant/account scope;
* ownership;
* allowed resources;
* sensitive fields.

Do not expose authorization tokens, internal credentials, secrets, or infrastructure identifiers through tool inputs or outputs.

Tool schemas must not be used as the only security boundary.

⸻

25. Tool annotations

Where supported by the MCP version/client, use tool annotations to accurately communicate behavioral properties such as whether a tool is read-only or potentially destructive.

Do not mark a tool as read-only if it can mutate state.

Annotations describe behavior; they do not replace authorization.

⸻

26. Avoid over-engineering

Do not use advanced JSON Schema features merely because MCP supports them.

Use oneOf, anyOf, conditionals, $defs, and $ref when they solve a real ambiguity or validation problem.

Prefer the simplest schema that accurately represents the business contract.

⸻

27. Evaluation is mandatory

A tool should be evaluated as an agent tool, not only as a JSON Schema.

Create an evaluation dataset containing:

Happy paths

User intent → expected tool call

Ambiguous requests

User intent → expected clarification or unfiltered call

Negative cases

User intent → tool must NOT be called

Boundary cases

dates
empty filters
conflicting filters
unknown terminology

Enum cases

For every enum:

* direct terminology;
* natural-language synonym;
* paraphrase;
* ambiguous wording;
* unrelated wording.

Measure:

* correct tool selection;
* correct parameter selection;
* correct enum selection;
* invalid-call rate;
* unnecessary-call rate;
* clarification rate;
* number of tool calls;
* response usefulness;
* token consumption.

⸻

28. Held-out evaluation

Do not optimize only against the examples included in the tool description.

Maintain:

development evaluation set
+
held-out evaluation set

The held-out set should contain realistic language variants that were not used to write the descriptions.

This prevents overfitting the schema to a handful of examples.

⸻

29. Tool review checklist

Before publishing a tool, verify:

* Tool has one clear responsibility.
* Tool name is explicit and unambiguous.
* Tool title is concise.
* Tool description explains what it does.
* Tool description explains when to use it.
* Tool description explains when NOT to use it.
* Domain-specific terminology is defined.
* Parameter names express business meaning.
* Every parameter has a useful description.
* Dates specify exactly what they represent.
* Defaults are intentional.
* Optional parameters are genuinely optional.
* Required parameters are genuinely required.
* additionalProperties is intentionally configured.
* Enums are semantically understandable.
* Enum values are stable canonical values.
* Enum titles are concise.
* Enum descriptions explain non-obvious business semantics.
* Enum ambiguity has been addressed.
* The schema does not require users to know technical enum names.
* API mappings exist only where they provide real value.
* MCP is not a blind API proxy.
* Output is high-signal and token-efficient.
* Errors are actionable.
* Authorization is enforced independently of schema validation.
* Tool annotations are accurate where applicable.
* Natural-language evaluations exist.
* Ambiguous and negative evaluations exist.
* Held-out evaluations exist.

⸻

30. Anti-patterns

API-as-MCP

OpenAPI → automatically expose every endpoint as an MCP tool

Problem:

The API is designed for deterministic software, not necessarily for LLM reasoning.

Cryptic enums

"01"
"02"
"03"

Problem:

The LLM has no semantic information.

Technical leakage

payment_status_code
internal_product_code
db_partition_key

Problem:

Implementation details become part of the agent contract.

Vague descriptions

"Gets payments."

Problem:

The agent does not know when or how to use the tool.

Enum dumping

description:
"Possible values: A, B, C, D, E, F, G, H..."

Problem:

The schema becomes noisy and still may not explain semantic differences.

Hidden business rules

expired
too_expired

without explaining the distinction.

Problem:

The LLM must guess.

Over-inference

User: "Show my payments."
Tool call:
paymentType = PIX

Problem:

The agent invented a filter.

Unbounded output

Returning thousands of records when the user needs a few.

Problem:

Consumes context and reduces downstream reliability.

⸻

31. Recommended modeling pattern

For a typical MCP tool:

{
  "name": "search_<business_resource>",
  "title": "<Human-readable title>",
  "description": "<What it does + when to use + when not to use>",
  "inputSchema": {
    "type": "object",
    "additionalProperties": false,
    "properties": {
      "<businessParameter>": {
        "type": "<type>",
        "title": "<Short label>",
        "description": "<Meaning + behavior + constraints>"
      }
    }
  }
}

For a semantic enum:

{
  "type": "string",
  "title": "<Field label>",
  "description": "<What the filter means and when to use it>",
  "oneOf": [
    {
      "const": "<CANONICAL_VALUE>",
      "title": "<Short human label>"
    }
  ]
}

If the enum’s meaning is not obvious, enrich the option with the semantic context supported by the target schema/client, or place the necessary explanation in the field description.

⸻

32. Final design rule

When reviewing an MCP tool, ask:

Could a capable LLM, seeing only the tool definition and the user’s request, correctly determine whether to call this tool, what parameters to provide, which enum values to select, and what those values mean—without consulting internal API documentation?

If the answer is no, the MCP contract is incomplete.

The goal is not to make the tool schema mirror the API.

The goal is to make the tool easy for an agent to use correctly while preserving a stable and maintainable boundary to the underlying systems.