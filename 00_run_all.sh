# Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
# Last modified: 2025.11.20

# This script collects metadata on individuals in the yearly person networks into one single comma-separated
# table per year. These files can be used as node attribute tables for mlnlib multilayer network objects.

# Metadata is on
#     - basic demographic information (gender, birth year, migrant generation, missing information on parents)
#     - individual and household income (only after 2011)
#     - highest education level
#     - address location up to buurt level

# It combines individual-level source files from Microdata with education, municipality, and buurt metadata information.

working_folder="H:\\ODISSEI_portal_C"
start_year=2009
end_year=2023

log_file="$working_folder/log_messages_$(date +'%Y-%m-%d_%H-%M-%S').log"
error_file="$working_folder/log_error_$(date +'%Y-%m-%d_%H-%M-%S').log"
echo -e "RUN $(date +'%Y-%m-%d_%H-%M-%S')\n\n" >$log_file
echo -e "RUN $(date +'%Y-%m-%d_%H-%M-%S')\n\n" >$error_file

echo -e "==============================\n 00 Creating temp and output folders. \n==============================" | tee -a $log_file $error_file
if [ ! -d $working_folder"/temp" ]; then
    mkdir $working_folder"/temp"
fi
if [ ! -d $working_folder"/codebook" ]; then
    mkdir $working_folder"/codebook"
fi
if [ ! -d $working_folder"/yearly_node_files" ]; then
    mkdir $working_folder"/yearly_node_files"
fi

echo -e "==============================\n 01 Creating population. \n==============================" | tee -a $log_file $error_file
/c/mambaforge/envs/9629/python.exe src/01_nodes_merged_nodelist.py $start_year $end_year $working_folder >>$log_file 2>>$error_file

for year in `seq $start_year $end_year`
do
    echo -e "==============================\n YEAR $year \n==============================" | tee -a $log_file $error_file

    echo -e "==============================\n 02 Yearly base populations. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/02_nodes_base_files.py $start_year $end_year $year $working_folder >>$log_file 2>>$error_file
    
    echo -e "==============================\n 03 Income (after 2011). \n==============================" | tee -a $log_file $error_file
    if [ "$year" -gt 2010 ]; then
        /c/mambaforge/envs/9629/python.exe src/03_nodes_income.py $start_year $end_year $year $working_folder >>$log_file 2>>$error_file
    else
        echo -e "Year $year has no income data." | tee -a $log_file $error_file
    fi
    
    echo -e "==============================\n 04 Education. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/04_nodes_education.py $year $working_folder >>$log_file 2>>$error_file
    
    echo -e "==============================\n 05 Location of address up to buurt. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/05_nodes_location.py $year $working_folder >>$log_file 2>>$error_file
    
    echo -e "==============================\n 06 Buurt info. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/06_buurt_metadata.py $year $working_folder >>$log_file 2>>$error_file
    
    echo -e "==============================\n 07 Gemeente info. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/07_gemeente_metadata.py $year $working_folder >>$log_file 2>>$error_file
    
    echo -e "==============================\n 08 Joining all the above files. \n==============================" | tee -a $log_file $error_file
    /c/mambaforge/envs/9629/python.exe src/08_combined_nodelists.py $start_year $end_year $year $working_folder >>$log_file 2>>$error_file
done

echo -e "==============================\n 08 Deleting temporary files. \n==============================" | tee -a $log_file $error_file
rm -rf $working_folder"/temp"  >>$log_file 2>>$error_file
echo -e "==============================\n Done. See results in ./yearly_node_files and ./codebook. \n==============================" | tee -a $log_file $error_file