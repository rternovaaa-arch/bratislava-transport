$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
Write-Host "Environment ready!" -ForegroundColor Green
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="$env:HADOOP_HOME\bin;$env:PATH"
