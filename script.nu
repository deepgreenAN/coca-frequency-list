def query [
    --words: string, # search terms
    --prefix, # search terms with specified prefix
    --suffix, # search terms with specified prefix
    --pos: string,  # filtering by parts of speech(pos)
    --sheet: int, # sheet number of frequency data
    --sorted: string,  # column name for sorting
    --skip: int, # skip number of rows
    --limit: int, # limit row number of query result
    --columns: string, # additional columns
    --all, # get all columns
] {

    mut args = []

    if $words != null {
        $args = ($args | prepend ["--words", $words])
    }
    if $prefix {
        $args = ($args | prepend "--prefix")
    }
    if $suffix {
        $args = ($args | prepend "--suffix")
    }
    if $pos != null {
        $args = ($args | prepend ["--pos", $pos])
    }
    if $sheet != null {
        $args = ($args | prepend ["--sheet", $sheet])
    }
    if $sorted != null {
        $args = ($args | prepend ["--sorted", $sorted])
    }
    if $skip != null {
        $args = ($args | prepend ["--skip", $skip])
    }
    if $limit != null {
        $args = ($args | prepend ["--limit", $limit])
    }
    if $columns != null {
        $args = ($args | prepend ["--columns", $columns])
    }
    if $all {
        $args = ($args | prepend "--all")
    }

    let temp_file_path = "./temp_query_result.csv";

    cargo run --quiet --release -- query $args --dist-path $temp_file_path

    open $temp_file_path
}

def sql [
    sql: string, # sql statement for query
    --sheets: string, # sheet numbers of frequency data
    --skip: int, # skip number of rows
    --limit: int, # limit row number of query result
] {
    mut args = []

    $args = ($args | prepend $sql)

    if $sheets != null {
        $args = ($args | prepend ["--sheets", $sheets])
    }
    if $skip != null {
        $args = ($args | prepend ["--skip", $skip])
    }
    if $limit != null {
        $args = ($args | prepend ["--limit", $limit])
    }

    let temp_file_path = "./temp_query_result.csv";

    cargo run --quiet --release -- sql $args --dist-path $temp_file_path

    open $temp_file_path
}