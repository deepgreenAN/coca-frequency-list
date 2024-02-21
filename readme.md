# coca frequency list

## Requirements

- nushell
- just

## Build

1. Download wordFrequency.xslx from official web site(wordfrequency.info): https://www.wordfrequency.info/samples.asp and locate it into "data" directory.
2. run the following command.

```shell
just build
```

## Usage

First, you need to enable "script.nu".

```shell
source script.nu
```

And you can select the frequency list and search it. You can get the 100 most frequent verb lemmas like this.

```shell
select_list "v" -e 100
```

You can search word forms in the "wordForms" sheet like this.

```shell
search_list have,has -t 3
```
