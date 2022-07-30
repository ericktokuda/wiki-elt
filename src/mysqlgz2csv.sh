#!/bin/bash
# Convert mysql to csv

function help {
    echo "
    Usage: $(basename -- $0) MYSQLTGZ OUTCSV
    Copy path to remote location
    -h, --help  (optional):  Print this help message
    MYSQLTGZ: Mysql compressed file (.tgz)
    OUTCSV:  Output csv file
    "
    exit -1
}

if [[ $# != 2 ]]; then
    help
elif [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    help
elif [[ ! -f "$1" ]]; then
    echo "File $1 does not exist. Aborting"
	exit -1
elif [[ -f $2 ]]; then
    echo "File $2 already exists. Aborting"
	exit -1
fi


function main {
	TMP=$HOME/temp/deleteme

	CMD="zcat $1 | grep $'^INSERT INTO ' | sed 's/),(/\n/g' > $TMP"
    echo "$CMD" && eval "$CMD"

	CMD1="head $TMP -n1 |  sed 's/.*(//'"

	CMD="{ $CMD1; sed '1d' $TMP; } > $2 && rm $TMP"
    echo "$CMD" && eval "$CMD"

    echo "Success. Check $2 ."
}

main "$@"

