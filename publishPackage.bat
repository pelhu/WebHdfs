dotnet build src\WebHdfs.Core -c Release
dotnet pack src\WebHdfs.Core -c Release

FOR %%F IN (src\WebHdfs.Core\bin\Release\WebHdfs.Core.*.symbols.nupkg) DO (
 set filename=%%F
)
:_exitfor

\\172.17.7.16\Public\NugetPackages\nuget.exe add "%filename%" -Source \\172.17.7.16\Public\NugetPackages

pause