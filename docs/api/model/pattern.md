## Pattern type

> Note: {% include types-note.md %}

_Starting from version 0.16:_

- `id` (integer) - the ID of the pattern.
- `sourceCode` (string) - the pattern itself, written in TSPL.
- `subunit` (integer) - a sub-category ID for the pattern.

_Version 0.15 and earlier:_

- `id` (string) - the ID of the pattern.
- `sourceCode` (string) - the pattern itself, written in TSPL.
- `payload` (JSON object) - additional info for a pattern (in version 0.15, sub-unit should be included as value of `subunit` key).
- `forwardedFields` (JSON object) - fields of an incident which should be stored in the sink as additional context. 