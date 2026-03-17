# Releasing

This project publishes to npm automatically through GitHub Actions when a new Git tag is pushed.

## Release flow

1. Update the version in `src/package.json`:

```bash
cd src
npm version patch --no-git-tag-version
cd ..
```

2. Commit the version change:

```bash
git add .
git commit -m "release: v1.0.6"
```

3. Create the Git tag:

```bash
git tag v1.0.6
```

4. Push the branch and the tag:

```bash
git push origin main --tags
```

When the tag is pushed, GitHub Actions publishes the package to npm automatically.

## Notes

- The npm package lives in `src/`.
- The publish workflow runs from `src/`.
- Replace `patch` with `minor` or `major` when needed.
- The published package name is `@ricardohsmello/mongodb-cli-lab`.
