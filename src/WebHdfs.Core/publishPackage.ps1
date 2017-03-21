#$versionPattern = "(""version"": "".+?)-(\d+)("")"
#$prj = Get-Content $PSScriptRoot\project.json

#$version = [int]($prj| select-string $versionPattern).matches[0].Groups[2].Value

#$prj -replace $versionPattern,('$1-'+($version+1)+'$3') | Set-Content $PSScriptRoot\project.json

$prjName = Split-Path $PSScriptRoot -Leaf
$versionPattern = "(\<VersionPrefix\>.+?)-(\d+)(\<)"
$prjPath = "$PSScriptRoot\$prjName.csproj"
$prjContent = Get-Content $prjPath

$version = [int]($prjContent| select-string $versionPattern).matches[0].Groups[2].Value

$prjContent -replace $versionPattern,('$1-'+($version+1)+'$3') | Set-Content $prjPath

#dotnet build $PSScriptRoot\. -c Release
dotnet pack $PSScriptRoot\. -c Release --include-symbols

$filename = gci $PSScriptRoot\bin\Release\*.symbols.nupkg | sort Name | select -last 1

\\172.17.7.16\Public\NugetPackages\nuget.exe add "$($filename.FullName)" -Source \\172.17.7.16\Public\NugetPackages