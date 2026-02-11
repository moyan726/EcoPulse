Write-Host "Checking git status..."
git status

Write-Host "Adding all files..."
git add .

Write-Host "Committing changes..."
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
git commit -m "feat: Complete project lifecycle (Serving & Visualization) - $timestamp"

Write-Host "Pushing to remote..."
git push origin main

Write-Host "Done!"
Pause
