# Coding guidelines

### Design
- Prefer pure and simple FP design style over "Java-like" style as long
as this does not contradict the performance directly.

## Coding style
- Preserve project-level style you see around
- Add comments for complicated and not obvious parts of code
- Use Scalafmt for indentation (`.scalafmt.conf` in project root)
- Preserve reasonable, descriptive naming style (e.g. no one-letter
names except one-line lambdas)

## Changelog writing
- All information about bugs fixed and featured introduced for the
 version yet unreleased should be contained in the `CHANGELOG.wip` file.
 Don't modify the `CHANGELOG` file (without extension) by hand (it
 contains only the notes for the already released versions and is automatically
 updated from the `CHANGELOG.wip` file upon the release).