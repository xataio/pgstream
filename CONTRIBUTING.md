We welcome contributions from the community! If you'd like to contribute to pgstream, please follow these guidelines:

- Create an issue for any questions, bug reports, or feature requests.
- Check the documentation and existing issues before opening a new issue.

## Contributing Code

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and write tests if applicable.
4. Ensure your code passes linting and tests.
   - There's a [pre-commit](https://pre-commit.com/) configuration available on the root directory (`.pre-commit-config.yaml`), which can be used to validate some of the correctness CI checks locally.
   - Use `make test` and `make integration-test` to validate unit and integration tests pass locally.
   - Use `make generate` to ensure the generated files are up to date.
5. Submit a pull request.

For this project, we pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.
