# Contributing to ParquetSharp.DataFrame

First off, thanks for taking the time to contribute!

All types of contributions are encouraged and valued. See the [Table of Contents](#table-of-contents) for different ways to help and details about how this project handles them. Please make sure to read the relevant section before making your contribution. It will make it a lot easier for us maintainers and smooth out the experience for all involved. The community looks forward to your contributions.

> And if you like the project, but just don't have time to contribute, that's fine. There are other easy ways to support the project and show your appreciation, which we would also be very happy about:
> - Star the project
> - Tweet about it
> - Refer this project in your project's readme
> - Mention the project at local meetups and tell your friends/colleagues

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [I Have a Question](#i-have-a-question)
- [I Want To Contribute](#i-want-to-contribute)
- [Reporting Bugs](#reporting-bugs)
- [Styleguides](#styleguides)

## Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to conduct.parquetsharp@gr-oss.io.

## I Have a Question

> If you want to ask a question, we assume that you have read the available [README.md](README.md).

Before you ask a question, it is best to search for existing [Issues](https://github.com/G-Research/ParquetSharp.DataFrame/issues) that might help you. In case you have found a suitable issue and still need clarification, you can write your question in this issue. It is also advisable to search the internet for answers first.

If you still need to ask a question and need clarification, we recommend the following:

- Open an [Issue](https://github.com/G-Research/ParquetSharp.DataFrame/issues/new).
- Provide as much context as you can about what you're running into.
- Provide the version of the project you are using.
- Provide the version of .NET you are using, as well as the version of the operating system you are using.
- Explain what you expected to happen and what actually happened.
- Explain what you did to solve the problem (if you did).

We will then take care of the issue as soon as possible.

## I Want To Contribute

> ### Legal Notice
> When contributing to this project, you must agree that you have authored 100% of the content, that you have the necessary rights to the content and that the content you contribute may be provided under the project license.

### Reporting Bugs

> We use GitHub issues to track bugs and errors. If you run into an issue with the project:

- Open an [Issue](https://github.com/G-Research/ParquetSharp.DataFrame/issues/new).
- Explain the behavior you would expect and the actual behavior.
- Please provide as much context as possible and describe the *reproduction steps* that someone else can follow to recreate the issue on their own. This usually includes your code.

Once it's filed:

- A team member will try to reproduce the issue with your provided steps. If there are no reproduction steps or no obvious way to reproduce the issue, the team will not be able to address your bug.

#### Before Submitting a Bug Report

A good bug report shouldn't leave others needing to chase you up for more information. Please complete the following steps in advance to help us fix any potential bug as fast as possible:

- Make sure that you are using the latest version.
- Determine if your bug is really a bug and not an error on your side, e.g., using incompatible environment components/versions. If you are looking for support, you might want to check [this section](#i-have-a-question).
- Check if a bug report already exists for your issue in the [bug tracker](https://github.com/G-Research/ParquetSharp.DataFrame/issues?q=label%3Abug).
- Search the internet (including Stack Overflow) to see if users outside of the GitHub community have discussed the issue.
- Collect information about the bug:
  - Stack trace (Traceback)
  - OS, Platform, and Version (Windows, Linux, macOS, x86, ARM)
  - Version of the interpreter, compiler, SDK, runtime environment, package manager, depending on what seems relevant.
  - Possibly your input and the output.
  - Can you reliably reproduce the issue? And can you also reproduce it with older versions?

## Styleguides

We encourage contributors to follow the project's code formatting standards. Before submitting a pull request, please ensure that your code adheres to the formatting guidelines specified in `.github/workflows/ci.yml`.

For this project, we use the following tools for code formatting:

```sh
dotnet tool restore
dotnet tool run dotnet-format -- --check
dotnet jb cleanupcode --profile="Built-in: Reformat Code" --settings="ParquetSharp.DataFrame.DotSettings" --verbosity=WARN "ParquetSharp.DataFrame" "ParquetSharp.DataFrame.Test"
```

## Attribution
This guide is based on the **contributing.md** guide from [contributing.md](https://contributing.md/).