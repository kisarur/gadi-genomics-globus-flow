wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.tumor.dna.R1.fastq.gz
wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.tumor.dna.R2.fastq.gz
wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.normal.dna.R1.fastq.gz
wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.normal.dna.R2.fastq.gz
wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.tumor.rna.R1.fastq.gz
wget https://github.com/nf-core/test-datasets/raw/oncoanalyser/sample_data/simulated_reads/wgts/fastq/single_lane_single_library/subject_a.tumor.rna.R2.fastq.gz
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/GRCh38_masked_exclusions_alts_hlas.fasta
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/samtools_index-1.16/GRCh38_masked_exclusions_alts_hlas.fasta.fai
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/samtools_index-1.16/GRCh38_masked_exclusions_alts_hlas.fasta.dict
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/bwa_index_image-gatk-4.6.1.0/GRCh38_masked_exclusions_alts_hlas.fasta.img
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/gridss_index-2.13.2.tar.gz
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/bwa-mem2_index-2.2.1.tar.gz
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/genomes/GRCh38_hmf/25.1/star_index-gencode_38-2.7.3a.tar.gz
wget https://pub-cf6ba01919994c3cbd354659947f74d8.r2.dev/hmf_reference_data/hmftools/hmf_pipeline_resources.38_v2.2.0--3.tar.gz

mkdir -p demo_data/GRCh38_hmf/25.1
mkdir -p demo_data/GRCh38_hmf/25.1/samtools_index-1.16
mkdir -p demo_data/GRCh38_hmf/25.1/bwa_index_image-gatk-4.6.1.0

mv subject_a.tumor.dna.R1.fastq.gz subject_a.tumor.dna.R2.fastq.gz subject_a.normal.dna.R1.fastq.gz subject_a.normal.dna.R2.fastq.gz subject_a.tumor.rna.R1.fastq.gz subject_a.tumor.rna.R2.fastq.gz hmf_pipeline_resources.38_v2.2.0--3.tar.gz demo_data/
mv GRCh38_masked_exclusions_alts_hlas.fasta gridss_index-2.13.2.tar.gz bwa-mem2_index-2.2.1.tar.gz star_index-gencode_38-2.7.3a.tar.gz demo_data/GRCh38_hmf/25.1/
mv GRCh38_masked_exclusions_alts_hlas.fasta.fai GRCh38_masked_exclusions_alts_hlas.fasta.dict demo_data/GRCh38_hmf/25.1/samtools_index-1.16/
mv GRCh38_masked_exclusions_alts_hlas.fasta.img demo_data/GRCh38_hmf/25.1/bwa_index_image-gatk-4.6.1.0/
