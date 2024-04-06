from pyspark.sql import Row, SparkSession


def create_data(spark):
    """Создание датафреймов."""

    products_df = spark.createDataFrame([
        Row(id=1, product_name='bread'),
        Row(id=2, product_name='limon'),
        Row(id=3, product_name='hummer'),
        Row(id=4, product_name='pencil'),
        Row(id=5, product_name='notebook'),
    ])

    categories_df = spark.createDataFrame([
        Row(id=1, category_name='food'),
        Row(id=2, category_name='fruits'),
        Row(id=3, category_name='tools'),
        Row(id=4, category_name='toys'),
        Row(id=5, category_name='clothes'),
    ])

    links_df = spark.createDataFrame([
        Row(product_id=1, category_id=1),
        Row(product_id=2, category_id=1),
        Row(product_id=2, category_id=2),
        Row(product_id=3, category_id=3),
    ])

    return products_df, categories_df, links_df


def get_products_categories(products_df, categories_df, links_df):
    """Выборка данных о всех категориях для продуктов."""

    full_table_products_left = (
        products_df
        .join(links_df, products_df['id'] == links_df['product_id'], 'left')
        .join(categories_df, links_df['category_id'] == categories_df['id'], 'left')
    )

    return full_table_products_left.select('product_name', 'category_name')


if __name__ == '__main__':
    frames = create_data(SparkSession.builder.getOrCreate())
    get_products_categories(*frames).show()
