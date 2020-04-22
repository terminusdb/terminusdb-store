# How to contribute

Thanks for taking the time to contribute to terminus-store!

## Testing
Run `cargo test` before submitting your changes and be sure to make a test for your change as well.

## Submitting changes
Please send a [GitHub Pull Request](https://github.com/terminusdb/terminus-store/pull/new/master) to the master branch.

Please write clear log messages with your commits. Small changes can be a one
line message, but big changes should have a descriptive paragraph with a
newline after the title in the message.

It should look something like this:

    $ git commit -m "My change title
    >
    > This is a paragraph describing my change."


## Coding conventions
We adhere to the formatting style used by `cargo fmt`. Please run `cargo fmt`
before submitting your changes as well.

## License of your contribution
Unless specified otherwise, we assume your contribution is contributed under
the same license as the rest of the project (GPLv3). If you wish to contribute
under another license, please make this clear in your pull request, and include
the required edits to the readme. Your change will still need to be
GPLv3-compatible, and we're likely to reject any pull request with a different
license unless there's a very good reason for doing so. A good reason might be
if you are merging a significant amount of code from another project which uses
another license, and you wish to ensure that the original project is able to
merge back patches.

If you need to pull in any extra dependencies, make sure these are licensed
under a GPLv3-compatible license. This is the case for any library licensed
under MIT, Apache v2, or both. Therefore, most libraries in the Rust FOSS
ecosystem should be fine.
