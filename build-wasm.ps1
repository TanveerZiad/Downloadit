$ErrorActionPreference = 'Stop'

$Rustup = Join-Path $env:USERPROFILE '.cargo\bin\rustup.exe'
$Cargo = Join-Path $env:USERPROFILE '.cargo\bin\cargo.exe'
$WasmBindgen = Join-Path $env:USERPROFILE '.cargo\bin\wasm-bindgen.exe'

& $Rustup target add wasm32-unknown-unknown
if ($LASTEXITCODE -ne 0) { throw 'Could not install the wasm32 Rust target.' }

& $Cargo install wasm-bindgen-cli --locked
if ($LASTEXITCODE -ne 0) { throw 'Could not install wasm-bindgen-cli.' }

& $Cargo build -p downloadit-web --release --target wasm32-unknown-unknown
if ($LASTEXITCODE -ne 0) { throw 'Could not compile the Rust frontend.' }

New-Item -ItemType Directory -Force web\pkg | Out-Null
& $WasmBindgen --target web --out-dir web\pkg --out-name downloadit_web target\wasm32-unknown-unknown\release\downloadit_web.wasm
if ($LASTEXITCODE -ne 0) { throw 'Could not generate the browser WebAssembly package.' }

Write-Host 'Rust WebAssembly frontend built in web/pkg.'
