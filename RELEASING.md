# Releasing

This project publishes to npm automatically through GitHub Actions when a new Git tag is pushed.

## Release flow

1. Update the version in `package.json`:

```bash
npm version patch --no-git-tag-version
```

2. Commit the version change:

```bash
git add .
git commit -m "release: v1.0.6"
```

3. Create the Git tag:

```bash
git tag v.1.0.6
```

4. Push the branch and the tag:

```bash
git push origin main --tags
```

When the tag is pushed, GitHub Actions publishes the package to npm automatically.

## Notes

- The npm package manifest lives in the repository root.
- The CLI entrypoint is `src/index.js`.
- Replace `patch` with `minor` or `major` when needed.
- The published package name is `@ricardohsmello/mongodb-cli-lab`.
