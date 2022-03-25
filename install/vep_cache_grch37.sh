#!/bin/bash
# Installs cache data for ensembl-vep

SPECIES="homo_sapiens"
ASSEMBLY="GRCh37"

echo "Install ensembl VEP cache data in cache directory"
perl /ensembl-vep/INSTALL.pl --NO_UPDATE --CACHEDIR ${VEP_CACHE_DIR} --AUTO cf --SPECIES $SPECIES --ASSEMBLY $ASSEMBLY

# VEP also requires the assembly FASTA file. For performance reasons it is
# recompressed as a block gzipped file and indexed
# Note: for the assembly GRCh37 the latest release version of ensembl is 75
FASTA_TOPLEVEL="Homo_sapiens.GRCh37.75.dna.toplevel.fa"
CACHE_PATH=${VEP_CACHE_DIR}/${SPECIES}/${VEP_ENSEMBL_VERSION}_${ASSEMBLY}

# note: rsync not available in this container. If ever changing the following
# to rsync, comply to ensembl.org recommended URL for rsync: https://useast.ensembl.org/info/data/ftp/rsync.html
echo "Downloading toplevel assembly"
wget -q ftp://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/dna/${FASTA_TOPLEVEL}.gz -P ${CACHE_PATH} -O ${FASTA_TOPLEVEL}.gz
echo "Unzip fasta file"
gzip -d ${CACHE_PATH}/${FASTA_TOPLEVEL}.gz
echo "Re-compress as blocked gzip"
bgzip -i ${CACHE_PATH}/${FASTA_TOPLEVEL}
echo "Index compressed file"
samtools faidx ${CACHE_PATH}/${FASTA_TOPLEVEL}.gz
