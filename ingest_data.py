import RetailAnalysis as ra

retail = ra.RetailAnalysis()

retail.create_neo4j_driver()

with retail.get_session() as session:
    # Start fresh by clearing all data from database
    # We're using the default database
    session.write_transaction(retail.clear_all_data)

    # Create nodes
    session.write_transaction(retail.ingest_orders_data_nodes)
    session.write_transaction(retail.ingest_customer_data_nodes)
    session.write_transaction(retail.ingest_products_data_nodes)
    session.write_transaction(retail.ingest_suppliers_data_nodes)
    session.write_transaction(retail.ingest_categories_data_nodes)
    session.write_transaction(retail.ingest_employees_data_nodes)

    # Create relationships between nodes
    session.write_transaction(retail.create_order_product_relationships)
    session.write_transaction(retail.create_customer_product_relationship)
    session.write_transaction(retail.create_supplier_product_relationship)
    session.write_transaction(retail.create_order_employee_relationship)
    session.write_transaction(retail.create_order_customer_relationship)
