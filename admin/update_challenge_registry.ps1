# usage: . .\update_challenge_registry.ps1

(Push-Location ..                                                                        ) -and
(Push-Location py_spark               && . .\update_challenge_registry.ps1 && Pop-Location) -and
(Push-Location python_dask            && . .\update_challenge_registry.ps1 && Pop-Location) -and
(Push-Location python_free_threaded       && . .\update_challenge_registry.ps1 && Pop-Location) -and
(Push-Location python_single_threaded  && . .\update_challenge_registry.ps1 && Pop-Location) -and
(deactivate                                                                              ) -and
(Pop-Location                                                                            ) -and
(Write-Output "Success"                                                                  )

