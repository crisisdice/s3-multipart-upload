#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 file partSizeInMb";
    exit 0;
fi

file=$1

if [ ! -f "$file" ]; then
    echo "Error: $file not found." 
    exit 1;
fi

partSizeInMb=$2
fileSizeInMb=$(du -m "$file" | cut -f 1)
parts=$((fileSizeInMb / partSizeInMb))
if [[ $((fileSizeInMb % partSizeInMb)) -gt 0 ]]; then
    parts=$((parts + 1));
fi

checksumFile=$(mktemp -t s3md5)

for (( part=0; part<$parts; part++ ))
do
    skip=$((partSizeInMb * part))
    $(dd bs=1m count=$partSizeInMb skip=$skip if="$file" 2>/dev/null | md5 >>$checksumFile)
done

echo $(xxd -r -p $checksumFile | md5)-$parts
rm $checksumFile
