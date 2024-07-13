Write-host '0'
python -m src.BiLevelPerfTest.BiLevelRunner --runs 5
python -m src.ConditionalPerfTest.CondRunner --runs 5
python -m src.VanillaPerfTest.VanillaRunner --runs 5
Write-host '1'
python -m src.DedupePerfTest.DedupeRunner --runs 5
python -m src.SectionPerfTest.SectionRunner --runs 5
Write-host '2'
python -m src.DedupePerfTest.DedupeRunner --runs 5
python -m src.SectionPerfTest.SectionRunner --runs 5
Write-host '3'
python -m src.DedupePerfTest.DedupeRunner --runs 5
python -m src.SectionPerfTest.SectionRunner --runs 5
