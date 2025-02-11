# Activar el entorno virtual y ejecutar el primer script
Set-Location C:\Users\pablo\Documents\ants
.\venv\Scripts\activate
$env:PYTHONPATH = "C:\Users\pablo\Documents\ants"

# Ejecutar el primer script de Python
try {
    python .\app\carga_bls.py
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Primer script ejecutado con éxito."

       # Ejecutar los cinco scripts en paralelo
       $scripts = @(
            "python .\app\rutina.py --diaria --naviera HAPAG-LLOYD",
            "python .\app\rutina.py --diaria --naviera CMA-CGM PIL",
            "python .\app\rutina.py --diaria --naviera ONE COSCO EVERGREEN WANHAI",
            "python .\app\rutina.py --diaria --naviera MSC ZIM YANGMING",
            "python .\app\rutina.py --diaria --naviera MAERSK HMM"
        )

        $processes = @()

        foreach ($script in $scripts) {
            $process = Start-Process -FilePath "powershell.exe" -ArgumentList "-Command $script" -PassThru
            $processes += $process
        }

        # Esperar a que todos los procesos terminen
        foreach ($process in $processes) {
            $process.WaitForExit()
            if ($process.ExitCode -ne 0) {
                throw "Error en uno de los scripts en paralelo"
            }
        }
        # Ejecutar el script final si todos los scripts en paralelo terminan con éxito
        python .\app\actualizar_output.py
        python .\app\enviar_correo.py -m "Rutina terminada"

    } else {
        throw "Error en el primer script"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Host "Error detectado: $errorMessage"
    # Ejecutar el script de error con el mensaje de error como argumento
    python .\app\actualizar_output.py
    python .\app\enviar_correo.py -m "$errorMessage"
}
