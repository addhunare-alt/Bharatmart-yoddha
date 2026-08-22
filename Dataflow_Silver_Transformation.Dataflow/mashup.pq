[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a981bd64-f51b-44e8-bb5e-b8f9142f72b2"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_customers", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"customer_id", Order.Ascending}}),
  #"Replaced value" = Table.ReplaceValue(#"Sorted rows", null, "gen2@bootcamp.com", Replacer.ReplaceValue, {"email"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value", {{"customer_id", each Text.Trim(_), type nullable text}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Trimmed text", {{"customer_id", Int64.Type}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed duplicates", {{"birth_date", type date}}, "en-IN"),
  #"Added index" = Table.AddIndexColumn(#"Changed column type 1", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "customer_id", "customer_name", "email", "city", "birth_date", "updated_at"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"Index"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_products_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_products = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a981bd64-f51b-44e8-bb5e-b8f9142f72b2"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_products", ItemKind = "Table"]}[Data],
  #"Inserted trimmed text" = Table.AddColumn(#"Navigation 2", "Trim", each Text.Trim([product_name]), type nullable text),
  #"Replaced value" = Table.ReplaceValue(#"Inserted trimmed text", null, "Other Category", Replacer.ReplaceValue, {"category"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value", {{"product_id", type number}, {"price", type number}}, "en-IN"),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"product_id", "category", "price", "list_price", "Trim", "product_name"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Trim", "Product name"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns", {"product_id", "Product name", "category", "price", "list_price", "product_name"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Reordered columns 1", {{"list_price", type number}}, "en-IN")
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_stores_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_stores = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a981bd64-f51b-44e8-bb5e-b8f9142f72b2"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_stores", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"store_id", Int64.Type}}),
  #"Capitalized each word" = Table.TransformColumns(#"Changed column type", {{"zone", each Text.Proper(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Capitalized each word", " East", "East", Replacer.ReplaceText, {"zone"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", " North", "North", Replacer.ReplaceText, {"zone"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "East", "E", Replacer.ReplaceText, {"zone"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 2", {{"zone", each Text.Trim(_), type nullable text}}),
  #"Replaced value 3" = Table.ReplaceValue(#"Trimmed text", "E", "East", Replacer.ReplaceText, {"zone"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 3", {{"open_date", type date}}, "en-IN")
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_targets_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_targets = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a981bd64-f51b-44e8-bb5e-b8f9142f72b2"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_targets", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"target_id", type number}, {"target_amount", type number}, {"year", type number}, {"month", type number}}, "en-IN")
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared fact_sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a981bd64-f51b-44e8-bb5e-b8f9142f72b2"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_sales", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"sale_id", type number}, {"customer_id", type number}, {"product_id", type number}, {"store_id", type number}, {"quantity", Int64.Type}, {"unit_price", type number}, {"amount", type number}, {"sale_date", type date}}, "en-IN")
in
  #"Changed column type";
shared fact_sales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a6b19f06-c7b6-4b87-9216-218490d6355d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "fact_sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_customers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a6b19f06-c7b6-4b87-9216-218490d6355d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_products_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a6b19f06-c7b6-4b87-9216-218490d6355d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_products", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_stores_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a6b19f06-c7b6-4b87-9216-218490d6355d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_stores", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_targets_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "52527b90-6e37-41eb-b962-eabd19edbe03"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a6b19f06-c7b6-4b87-9216-218490d6355d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Gen2_Silver", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_targets", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
