#!/usr/bin/env bash

function clean_outer_join_columns(){
    local join_columns_list join_column join_column_coalesced joined_output cleaned_joined_output

    join_column="$1"
    join_column_coalesced="${join_column}_coalesced"
    joined_output="$(csvjoin -c "${join_column}" --outer *.csv 2>/dev/null)"

    ## Remove extraneous join columns since csvjoin does not have an option to do so for an outer join
    join_columns_list="$(echo "${joined_output}"| csvcut -n | awk '/'"${join_column}"'/ {print $2}' | perl -0777 -pe 's/[ ]*//g; s/\n/,/g; s/,$//g;')"
    printf "List of extraneous join columns which will be coalesced into ${join_column_coalesced} : %s\n\n" "${join_columns_list}"

    cleaned_joined_output="$( echo "${joined_output}" \
        | csvsql  --no-inference --query "select COALESCE($join_columns_list) as ${join_column_coalesced},* from stdin" \
        | csvcut  --delete-empty-rows --not-columns "$join_columns_list" \
        | csvsql  --query "select ${join_column_coalesced} as $join_column, * from stdin" \
        | csvcut  --delete-empty-rows --not-columns "${join_column_coalesced}")"

    printf "%s" "${cleaned_joined_output}" | csvlook
}

clean_outer_join_columns "$@"
