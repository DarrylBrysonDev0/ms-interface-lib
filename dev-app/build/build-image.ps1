$parentDir = Split-Path -Path $PSScriptRoot
$buildTimestamp = Get-Date -Format "_MMddyyyyHHmm"

function Set-PsEnv {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
    param($localEnvFile = "$parentDir\.env")

    #return if no env file
    if (!( Test-Path $localEnvFile)) {
        Throw "could not open $localEnvFile"
    }

    #read the local env file
    $content = Get-Content $localEnvFile -ErrorAction Stop
    Write-Verbose "Parsed .env file"

    #load the content to environment
    foreach ($line in $content) {
        if ($line.StartsWith("#")) { continue };
        if ($line.Trim()) {
            $line = $line.Replace("`"","")
            $kvp = $line -split "=",2
            if ($PSCmdlet.ShouldProcess("$($kvp[0])", "set value $($kvp[1])")) {
                [Environment]::SetEnvironmentVariable($kvp[0].Trim(), $kvp[1].Trim(), "Process") | Out-Null
            }
        }
    }
}

Write-Output " [*] Setting env variables."
Set-PsEnv

# Set-Location -Path $parentDir

$imageFile = $parentDir + "\Dockerfile"
$imageVersion = ":" + [Environment]::GetEnvironmentVariable("APP_VERSION", "Process") + $buildTimestamp
$imageName = [Environment]::GetEnvironmentVariable("APP_NAME", "Process")  #+ $imageVersion
$loginSrv = [Environment]::GetEnvironmentVariable("LOCAL_IMAGE_REG", "Process") #"192.168.86.33:5000" #"ghost-server-brysonlabs:5000" # adjust for use of private registry
# $imageTag = "$loginSrv/$imageName"

$versionTag = "$loginSrv/$imageName" + $imageVersion
$latestTag = "$loginSrv/$imageName" + ":latest"

Write-Output " [I] Version Tag:" + $versionTag

Write-Output " [*] Building image."
docker build --rm --build-arg LOCAL_IMAGE_REG=$loginSrv --no-cache -f $imageFile -t $imageName $parentDir
#docker build --rm -f ./app-image/Dockerfile.python-app-loop -t $imageName ./app-image

docker tag $imageName $versionTag
docker push $versionTag

docker tag $imageName $latestTag
docker push $latestTag