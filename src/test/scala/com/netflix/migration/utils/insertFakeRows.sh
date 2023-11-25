#!/bin/bash

# Set LC_CTYPE to C.UTF-8
export LC_CTYPE=C.UTF-8

# Check if argument is provided
if [ $# -eq 0 ]
then
    # Prompt user for number of rows to insert
    echo -n "Enter number of rows to insert: "
    read num_rows
else
    # Use argument as number of rows to insert
    num_rows=$1
fi

# Generate insert statement with fake data
echo "INSERT INTO prodhive.sandbox.job_sla_edges VALUES"
for (( i=1; i<=$num_rows; i++ ))
do
    id="$i"
    rand=$(openssl rand -hex 18 | base64)
    label=${rand:0:6}
    from_id=${rand:6:6}
    to_id=${rand:12:6}
    from_name=${rand:18:6}
    to_name=${rand:24:6}
    properties="map('property1', 'value1', 'property2', 'value2')"
    dateint=$(( (RANDOM % 100) + 1616329200 ))
    cluster_name="cluster_name$i"

    if [ $i -eq $num_rows ]
    then
        # Last row, so end with semicolon
        echo "('$id', '$label', '$from_id', '$to_id', '$from_name', '$to_name', $properties, $dateint, '$cluster_name');" 
    else
        # Not the last row, so end with comma
        echo "('$id', '$label', '$from_id', '$to_id', '$from_name', '$to_name', $properties, $dateint, '$cluster_name'),"
    fi
done

