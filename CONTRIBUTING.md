# Contributing to ParquetSharp.DataFrame

Thank you for your interest in contributing! 🎉

This library is primarily built and maintained to serve G-Research’s needs. While we welcome contributions, our main focus is ensuring the project continues to meet our requirements. We are more likely to accept bug fixes and documentation updates but may be conservative about new features.

We will review issues and pull requests on a **best-effort** basis.

## Issues

Please report bugs and feature requests by opening an [Issue](https://github.com/G-Research/ParquetSharp.DataFrame/issues). If possible, check for existing issues before submitting a new one.

For security-related issues, **do not** open a public issue. Instead, refer to our [security policy](https://github.com/G-Research/ParquetSharp.DataFrame/blob/main/SECURITY.md).

## Pull Requests

Before making large changes, open an issue to discuss your proposal. To increase the likelihood of acceptance:
- Keep changes small and focused
- Include tests for your modifications
- Follow project coding standards
- Provide clear explanations for design choices

## Code Style

Ensure your code adheres to the project's formatting guidelines using:
```sh
dotnet tool restore
dotnet tool run dotnet-format -- --check
dotnet jb cleanupcode --profile="Built-in: Reformat Code" --settings="ParquetSharp.DataFrame.DotSettings" --verbosity=WARN "ParquetSharp.DataFrame" "ParquetSharp.DataFrame.Test"
```

Thank you for contributing! 🚀

