def get_config():
    """Get configuration from environment variables."""
    return {
        'query_url': os.environ.get('QUERY_SERVICE_URL'),
        'token': os.environ.get('KBC_TOKEN'),
        'branch_id': os.environ.get('BRANCH_ID'),
        'workspace_id': os.environ.get('WORKSPACE_ID')
    }


def get_client():
    """Create Query Service client."""
    config = get_config()
    
    if not config['token']:
        raise RuntimeError("Missing KBC_TOKEN")
    if not config['workspace_id']:
        raise RuntimeError("Missing WORKSPACE_ID")
    
    return Client(
        base_url=config['query_url'],
        token=config['token']
    )


def execute_query(sql):
    """Execute a SQL query and return results."""
    config = get_config()
    
    with get_client() as client:
        results = client.execute_query(
            branch_id=config['branch_id'],
            workspace_id=config['workspace_id'],
            statements=[sql]
        )
        
        if results and len(results) > 0:
            result = results[0]
            # Convert to list of dicts
            columns = [col.name for col in result.columns]
            rows = []
            for row_data in result.data:
                rows.append(dict(zip(columns, row_data)))
            return rows
        return []


def execute_statement(sql):
    """Execute a SQL statement (INSERT, UPDATE, DELETE) without returning results."""
    config = get_config()
    
    with get_client() as client:
        client.execute_query(
            branch_id=config['branch_id'],
            workspace_id=config['workspace_id'],
            statements=[sql]
        )


# Table configuration
INVENTORY_TABLE = '"in.c-demo"."inventory"'


# -----------------------------------------------------------------------------
# Database Operations
# -----------------------------------------------------------------------------

def fetch_products(search=None, category=None):
    """Fetch all products with optional filtering."""
    query = f'SELECT * FROM {INVENTORY_TABLE}'
    conditions = []
    
    if search:
        # Escape single quotes in search
        safe_search = search.replace("'", "''")
        conditions.append(f"(name ILIKE '%{safe_search}%' OR id ILIKE '%{safe_search}%')")
    if category and category != 'all':
        safe_category = category.replace("'", "''")
        conditions.append(f"category = '{safe_category}'")
    
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    
    query += ' ORDER BY name ASC'
    
    return execute_query(query)


def fetch_categories():
    """Fetch distinct categories."""
    query = f'SELECT DISTINCT category FROM {INVENTORY_TABLE} ORDER BY category'
    rows = execute_query(query)
    return [row['CATEGORY'] for row in rows]


def get_product(product_id):
    """Fetch a single product by ID."""
    safe_id = product_id.replace("'", "''")
    query = f"SELECT * FROM {INVENTORY_TABLE} WHERE id = '{safe_id}'"
    rows = execute_query(query)
    return rows[0] if rows else None


def create_product(data):
    """Create a new product using INSERT."""
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Escape values
    safe_id = data['id'].replace("'", "''")
    safe_name = data['name'].replace("'", "''")
    safe_category = data['category'].replace("'", "''")
    quantity = int(data['quantity'])
    price = float(data['price'])
    
    sql = f"""
        INSERT INTO {INVENTORY_TABLE} (id, name, category, quantity, price, last_updated)
        VALUES ('{safe_id}', '{safe_name}', '{safe_category}', {quantity}, {price}, '{now}')
    """
    
    execute_statement(sql)
    
    return {
        'id': data['id'],
        'name': data['name'],
        'category': data['category'],
        'quantity': quantity,
        'price': price,
        'last_updated': now
    }


def update_product(product_id, data):
    """Update an existing product using UPDATE."""
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Escape values
    safe_id = product_id.replace("'", "''")
    safe_name = data['name'].replace("'", "''")
    safe_category = data['category'].replace("'", "''")
    quantity = int(data['quantity'])
    price = float(data['price'])
    
    sql = f"""
        UPDATE {INVENTORY_TABLE}
        SET name = '{safe_name}',
            category = '{safe_category}',
            quantity = {quantity},
            price = {price},
            last_updated = '{now}'
        WHERE id = '{safe_id}'
    """
    
    execute_statement(sql)
    
    return {
        'id': product_id,
        'name': data['name'],
        'category': data['category'],
        'quantity': quantity,
        'price': price,
        'last_updated': now
    }


def delete_product(product_id):
    """Delete a product using DELETE."""
    safe_id = product_id.replace("'", "''")
    sql = f"DELETE FROM {INVENTORY_TABLE} WHERE id = '{safe_id}'"
    
    try:
        execute_statement(sql)
        return True
    except Exception as e:
        app.logger.error(f"Delete failed: {e}")
        return False


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])  # Handle POST for Keboola startup check
def index():
    """Main inventory page."""
    return render_template('index.html')


@app.route('/api/products', methods=['GET'])
def api_list_products():
    """API: List products with optional filters."""
    search = request.args.get('search', '')
    category = request.args.get('category', 'all')
    
    try:
        products = fetch_products(search=search, category=category)
        categories = fetch_categories()
        
        # Normalize column names (Snowflake returns uppercase)
        normalized_products = []
        for p in products:
            normalized_products.append({
                'id': p.get('ID') or p.get('id'),
                'name': p.get('NAME') or p.get('name'),
                'category': p.get('CATEGORY') or p.get('category'),
                'quantity': p.get('QUANTITY') or p.get('quantity'),
                'price': p.get('PRICE') or p.get('price'),
                'last_updated': p.get('LAST_UPDATED') or p.get('last_updated')
            })
        
        return jsonify({
            'success': True,
            'products': normalized_products,
            'categories': categories,
            'total': len(normalized_products)
        })
    except Exception as e:
        app.logger.error(f"Failed to fetch products: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/products', methods=['POST'])
def api_create_product():
    """API: Create a new product."""
    data = request.json
    
    # Validate required fields
    required = ['id', 'name', 'category', 'quantity', 'price']
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({
            'success': False,
            'error': f'Missing required fields: {", ".join(missing)}'
        }), 400
    
    # Check for duplicate ID
    existing = get_product(data['id'])
    if existing:
        return jsonify({
            'success': False,
            'error': f'Product with ID {data["id"]} already exists'
        }), 400
    
    try:
        product = create_product(data)
        return jsonify({'success': True, 'product': product})
    except Exception as e:
        app.logger.error(f"Failed to create product: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/products/<product_id>', methods=['GET'])
def api_get_product(product_id):
    """API: Get a single product."""
    try:
        product = get_product(product_id)
        if not product:
            return jsonify({'success': False, 'error': 'Product not found'}), 404
        return jsonify({'success': True, 'product': product})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/products/<product_id>', methods=['PUT'])
def api_update_product(product_id):
    """API: Update an existing product."""
    data = request.json
    
    # Check product exists
    existing = get_product(product_id)
    if not existing:
        return jsonify({'success': False, 'error': 'Product not found'}), 404
    
    # Normalize existing data (handle uppercase column names)
    existing_normalized = {
        'name': existing.get('NAME') or existing.get('name'),
        'category': existing.get('CATEGORY') or existing.get('category'),
        'quantity': existing.get('QUANTITY') or existing.get('quantity'),
        'price': existing.get('PRICE') or existing.get('price')
    }
    
    # Merge with existing data
    updated_data = {
        'name': data.get('name', existing_normalized['name']),
        'category': data.get('category', existing_normalized['category']),
        'quantity': data.get('quantity', existing_normalized['quantity']),
        'price': data.get('price', existing_normalized['price'])
    }
    
    try:
        product = update_product(product_id, updated_data)
        return jsonify({'success': True, 'product': product})
    except Exception as e:
        app.logger.error(f"Failed to update product: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/products/<product_id>', methods=['DELETE'])
def api_delete_product(product_id):
    """API: Delete a product."""
    # Check product exists
    existing = get_product(product_id)
    if not existing:
        return jsonify({'success': False, 'error': 'Product not found'}), 404
    
    try:
        success = delete_product(product_id)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({
                'success': False,
                'error': 'Delete operation failed'
            }), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health')
def health():
    """Health check endpoint."""
    try:
        config = get_config()
        return jsonify({
            'status': 'healthy',
            'workspace_id': config['workspace_id'],
            'branch_id': config['branch_id'],
            'query_url': config['query_url']
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

