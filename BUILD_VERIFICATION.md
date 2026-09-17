# Build verification note

The repository was structurally reviewed after the shared logging refactor.

The packaging environment used to prepare this archive does not have the .NET SDK installed, so `dotnet build` could not be executed here.

Before the first GitHub push, run:

```powershell
dotnet restore .\PaymentService\PaymentService.csproj
dotnet build .\PaymentService\PaymentService.csproj

dotnet build .\InventoryService\InventoryService.csproj
dotnet build .\WarehouseService\WarehouseService.csproj
dotnet build .\ReprocessorService\ReprocessorService.csproj
dotnet build .\OrderProducer\OrderProducer.csproj
```

Then run the POC2 demo using the order in `README.md`.
