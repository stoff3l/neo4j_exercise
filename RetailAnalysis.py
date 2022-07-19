from neo4j import GraphDatabase


class RetailAnalysis:
    def create_neo4j_driver(self):
        # Create driver instance (unencrypted connection)
        driver = GraphDatabase.driver(
            uri="neo4j://localhost:7687",
            auth=("neo4j", "1234")
        )

        # Check that connection to neo4j is valid
        driver.verify_connectivity()

        self.driver = driver

    def get_session(self):
        return self.driver.session()

    def clear_all_data(self, tx):
        return tx.run("""
        MATCH (n)
        DETACH DELETE n
      """)

    def ingest_orders_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-orders.csv' AS line
        CREATE (:Order {
          orderID: toInteger(line.orderID),
          customerID: line.customerID,
          employeeID: toInteger(line.employeeID),
          orderDate: datetime(replace(line.orderDate, ' ', 'T')),
          shipVia: line.shipVia,
          freight: toFloat(line.freight),
          shipName: line.shipName,
          shipAddress: line.shipAddress,
          shipCity: line.shipCity,
          shipRegion: line.shipRegion,
          shipPostalCode: line.shipPostalCode,
          shipCountry: line.shipCountry
        })
      """)

    def ingest_customer_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-customers.csv' AS line
        CREATE (:Customer {
          customerID: line.customerID,
          companyID: line.companyID,
          contactName: line.contactName,
          contactTitle: line.contactTitle,
          address: line.address,
          region: line.region,
          postalCode: line.postalCode,
          country: line.country,
          phone: line.phone,
          fax: line.fax
        })
      """)

    def ingest_products_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-products.csv' AS line
        CREATE (:Product {
          productID: toInteger(line.productID),
          productName: line.productName,
          supplierID: toInteger(line.supplierID),
          categoryID: toInteger(line.categoryID),
          quantityPerUnit: line.quantityPerUnit,
          unitPrice: toFloat(line.unitPrice),
          unitsInStock: toInteger(line.unitsInStock),
          unitsOnOrder: toInteger(line.unitsOnOrder),
          reorderLevel: toInteger(line.reorderLevel),
          reorderLevel: toInteger(line.discontinued)
        })
      """)

    def ingest_suppliers_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-suppliers.csv' AS line
        CREATE (:Supplier {
          supplierID: toInteger(line.supplierID),
          companyName: line.companyName,
          contactName: line.contactName,
          address: line.address,
          city: line.city,
          region: line.region,
          postalCode: line.postalCode,
          country: line.country,
          phone: line.phone,
          fax: line.fax,
          homePage: line.homePage
        })
      """)

    def ingest_categories_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-categories.csv' AS line
        CREATE (:Category {
          categoryID: toInteger(line.categoryID),
          categoryName: line.categoryName,
          description: line.description,
          picture: line.picture
        })
      """)

    def ingest_employees_data_nodes(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwind-employees.csv' AS line
        CREATE (:Employee {
          employeeID: toInteger(line.employeeID),
          lastName: line.lastName,
          firstName: line.firstName,
          title: line.title,
          titleOfCourtesy: line.titleOfCourtesy,
          birthDate: datetime(replace(line.birthDate, ' ', 'T')),
          hireDate: datetime(replace(line.hireDate, ' ', 'T')),
          address: line.address,
          city: line.city,
          region: line.region,
          postalCode: line.postalCode,
          country: line.country,
          homePhone: line.homePhone,
          extension: line.extension,
          photo: line.photo,
          notes: line.notes
        })
      """)

    def create_order_product_relationships(self, tx):
        return tx.run("""
        LOAD CSV WITH HEADERS FROM 'file:///northwond-order-details.csv' AS line
        WITH toInteger(line.orderID) AS orderID,
             toInteger(line.productID) AS productID,
             toFloat(line.unitPrice) AS unitPrice,
             toInteger(line.quantity) AS quantity,
             toFloat(line.discount) AS discount
        MATCH (order:Order {orderID: orderID})
        MATCH (product:Product {productID: productID})
        MERGE (order)-[r:ORDERED {quantity: quantity}]->(product)
      """)

    def create_customer_product_relationship(self, tx):
        return tx.run("""
        MATCH (product:Product)
        MATCH (category:Category {categoryID: product.categoryID})
        MERGE (product)-[r:IN_CATEGORY]->(category)
      """)

    def create_supplier_product_relationship(self, tx):
        return tx.run("""
        MATCH (product:Product)
        MATCH (supplier:Supplier {supplierID: product.supplierID})
        MERGE (product)-[r:SUPPLIED_BY]->(supplier)
      """)

    def create_order_employee_relationship(self, tx):
        return tx.run("""
        MATCH (employee:Employee)
        MATCH (order:Order {employeeID: employee.employeeID})
        MERGE (order)-[r:ASSISTED_BY]->(employee)
      """)

    def create_order_customer_relationship(self, tx):
        return tx.run("""
        MATCH (customer:Customer)
        MATCH (order:Order {customerID: customer.customerID})
        MERGE (order)-[r:ORDERED_BY]->(customer)
      """)
