const POS_LIST = [
    "a",  # 冠詞
    "c",  # 接続詞
    "d", 
    "e",
    "i",  # 前置詞
    "j",  # 形容詞 
    "n",  # 名詞
    "p",  # 代名詞
    "r",  # 副詞
    "v"   # 動詞
];

const SUBJECT_LIST = [
    "blog",
    "web",
    "TVM",
    "spok",
    "fic",
    "mag",
    "news",
    "acad",
];

const SOURCE_TYPE_MAP = {
    "1" : "lemmas",
    "2" : "subgenres",
    "3" : "wordForms",
    "4" : "forms",
};

def select_list [
    pos?: string,  # a part of speech
    --start (-s): int,
    --end (-e): int,
    --genre (-g): string,  # genre for sort
] {
    if $pos != null and $pos not-in $POS_LIST {
        error make { msg: "unknown pos."}
    }
    if $genre != null and $genre not-in $SUBJECT_LIST {
        error make { msg: "unknown subject."}
    }

    let table_name = $SOURCE_TYPE_MAP | get "1";

    let sql = if $genre == null and $pos == null {
        $"
        SELECT lemma, freq FROM \"($table_name)\"
        "
    } else if $genre == null and $pos != null {
        $"
        SELECT lemma, freq FROM \"($table_name)\" WHERE \"PoS\" = '($pos)'
        "
    } else if $genre != null and $pos == null {
        $"
        SELECT lemma, freq, \"($genre)\" FROM \"($table_name)\" ORDER BY \"($genre)\" DESC
        "
    } else {
        $"
        SELECT lemma, freq, \"($genre)\" FROM \"($table_name)\" WHERE \"PoS\" = '($pos)' ORDER BY \"($genre)\" DESC
        "
    };

    let temp_file_path = "./temp_query_result.csv";

    if $start == null and $end == null {
        cargo run --release --quiet -- select  --dist-path $temp_file_path --sql $sql --source-type 1
    } else if $start != null and $end == null {
        cargo run --release --quiet -- select  --dist-path $temp_file_path --sql $sql --start $start --source-type 1
    } else if $start == null and $end != null {
        cargo run --release --quiet -- select --dist-path $temp_file_path --sql $sql --end $end --source-type 1
    } else if $start != null and $end != null {
        cargo run --release --quiet -- select --dist-path $temp_file_path --sql $sql --start $start --end $end --source-type 1
    }

    open $temp_file_path
}

def search_list [
    words: string  # words to search
    --type (-t): int  # sheet number
] {

    if $type != null and (($type | into string) not-in ($SOURCE_TYPE_MAP | columns )) {
        error make { msg: "unknown sheet number."}
    }

    let words = $words | split row "," ;

    let temp_file_path = "./temp_query_result.csv";

    if $type == null {
        cargo run --release --quiet -- search $words --dist-path $temp_file_path
    } else {
        cargo run --release --quiet -- search $words --dist-path $temp_file_path --source-type $type
    }    
    open $temp_file_path
}