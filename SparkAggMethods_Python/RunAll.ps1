cd src; 
Write-host '0'
python -m BiLevelPerfTest.BiLevelRunner --runs 5
python -m ConditionalPerfTest.CondRunner --runs 5
python -m VanillaPerfTest.VanillaRunner --runs 5
Write-host '1'
python -m DedupePerfTest.DedupeRunner --runs 5
python -m SectionPerfTest.SectionRunner --runs 5
Write-host '2'
python -m DedupePerfTest.DedupeRunner --runs 5
python -m SectionPerfTest.SectionRunner --runs 5
Write-host '3'
python -m DedupePerfTest.DedupeRunner --runs 5
python -m SectionPerfTest.SectionRunner --runs 5
cd ..