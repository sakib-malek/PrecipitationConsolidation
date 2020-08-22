#!/bin/bash

echo "To run the project."
echo "Both datasets need to be in the same folder"
echo "To submit a job to spark shell. $ spark-submit project.py -o output_folder_path -i input_folder_path"
echo "Input and output folder path are optional"

echo "Press any y to continue to run a following command spark-submit project.py. You can also run it manually. Press any other key to quit"
count=0
while : ; do
read k
if [[ $k = [yY] ]] ; then
printf "\nSubmitting job\n"
spark-submit project.py
break
else
echo "quitting"
break
fi
done


