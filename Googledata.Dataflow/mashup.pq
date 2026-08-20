[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "GoogleSales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "GoogleSales_DataDestinationTransform"]]]}]
shared GoogleSales = let
  Source = Csv.Document(Web.Contents("https://drive.google.com/uc?export=download&id=1IhBa8byYlcLHM8qTi0Ty9nKTMDpDI9m0"), [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"sale_id", Int64.Type}, {"customer_id", Int64.Type}, {"product_id", Int64.Type}, {"store_id", Int64.Type}, {"sale_date", type text}, {"quantity", Int64.Type}, {"unit_price", type number}, {"amount", type number}}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Changed column type", {{"sale_date", type date}}, "en-IN")
in
  #"Changed column type with locale";
shared GoogleSales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce5c9a8b-70ca-4182-a467-a250c775f409"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "Googledrive"]}[Content],
  FileNavigation = Navigation_4{[Name = "GoogleSales.csv"]}?[Content]?
in
  FileNavigation;
shared GoogleSales_DataDestinationTransform = Csv.OutputConfiguration([Encoding = 65001, Delimiter = ","]);
