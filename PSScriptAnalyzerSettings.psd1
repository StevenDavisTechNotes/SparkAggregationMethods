# For more information on PSScriptAnalyzer settings see:
# https://github.com/PowerShell/PSScriptAnalyzer/blob/master/README.md#settings-support-in-scriptanalyzer
#
# You can see the predefined PSScriptAnalyzer settings here:
# https://github.com/PowerShell/PSScriptAnalyzer/tree/master/Engine/Settings
@{
    # IncludeRules = @(
    #     'PSAvoidDefaultValueSwitchParameter',
    #     'PSMisleadingBacktick',
    #     'PSMissingModuleManifestField',
    #     'PSReservedCmdletChar',
    #     'PSReservedParams',
    #     'PSShouldProcess',
    #     'PSUseApprovedVerbs',
    #     'PSAvoidUsingCmdletAliases',
    #     'PSUseDeclaredVarsMoreThanAssignments'
    # )
    ExcludeRules = @('PSUseDeclaredVarsMoreThanAssignments')
}