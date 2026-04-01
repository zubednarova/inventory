"""
Inventory Manager - Direct Storage Access Demo App
===================================================
A Keboola Data App demonstrating read/write capabilities via Query Service.
Users can view, add, edit, and delete products — all changes sync to Storage.

Storage Table: in.c-demo.inventory
"""

import os
import json
from datetime import datetime
from flask import Flask, render_template, request, jsonify

# Query Service client for Direct Storage Access
from keboola_query_service import Client

app = Flask(__name__)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

def get_workspace_id():
    """Get workspace ID from manifest or environment variable."""
    # First try the manifest file (production)
    manifest_path = os.environ.get(
        'KBC_WORKSPACE_MANIFEST_PATH',
        '/var/run/secrets/keboola.com/workspace/manifest.json'
    )
    if os.path.exists(manifest_path):
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
            return manifest.get('workspaceId')
    
    # Fall back to environment variable
    return os.environ.get('WORKSPACE_ID')


def get_query_client():
    """Initialize Query Service client."""
    kbc_url = os.environ.get('KBC_URL', 'https://connection.keboola.com')
    kbc_token = os.environ.get('KBC_TOKEN')
    workspace_id = get_workspace_id()
    
    if not kbc_token or not workspace_id:
        raise RuntimeError("Missing KBC_TOKEN or WORKSPACE_ID")
    
    return QueryServiceClient(
        host=kbc_url.replace('https://connection.', 'https://query.'),
        token=kbc_token,
        workspace_id=workspace_id
    )


# Table configuration
INVENTORY_TABLE = 'in.c-demo.inventory'


# -----------------------------------------------------------------------------
# Database Operations
# -----------------------------------------------------------------------------

def fetch_products(search=None, category=None):
    """Fetch all products with optional filtering."""
    client = get_query_client()
    
    query = f'SELECT * FROM "{INVENTORY_TABLE}"'
    conditions = []
    
    if search:
        conditions.append(f"(name ILIKE '%{search}%' OR id ILIKE '%{search}%')")
    if category and category != 'all':
        conditions.append(f"category = '{category}'")
    
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    
    query += ' ORDER BY name ASC'
    
    result = client.query(query)
    return result.get('rows', [])


def fetch_categories():
    """Fetch distinct categories."""
    client = get_query_client()
    query = f'SELECT DISTINCT category FROM "{INVENTORY_TABLE}" ORDER BY category'
    result = client.query(query)
    return [row['category'] for row in result.get('rows', [])]


def get_product(product_id):
    """Fetch a single product by ID."""
    client = get_query_client()
    query = f"SELECT * FROM \"{INVENTORY_TABLE}\" WHERE id = '{product_id}'"
    result = client.query(query)
    rows = result.get('rows', [])
    return rows[0] if rows else None


def create_product(data):
    """Create a new product."""
    client = get_query_client()
    
    now = datetime.utcnow().isoformat() + 'Z'
    
    # Prepare row data
    row = {
        'id': data['id'],
        'name': data['name'],
        'category': data['category'],
        'quantity': int(data['quantity']),
        'price': float(data['price']),
        'last_updated': now
    }
    
    # Use unload endpoint to write data
    client.unload(
        table_id=INVENTORY_TABLE,
        rows=[row],
        incremental=True,
        primary_key=['id']
    )
    
    return row


def update_product(product_id, data):
    """Update an existing product."""
    client = get_query_client()
    
    now = datetime.utcnow().isoformat() + 'Z'
    
    # Prepare updated row
    row = {
        'id': product_id,
        'name': data['name'],
        'category': data['category'],
        'quantity': int(data['quantity']),
        'price': float(data['price']),
        'last_updated': now
    }
    
    # Upsert via unload endpoint
    client.unload(
        table_id=INVENTORY_TABLE,
        rows=[row],
        incremental=True,
        primary_key=['id']
    )
    
    return row


def delete_product(product_id):
    """Delete a product by ID."""
    client = get_query_client()
    
    query = f"DELETE FROM \"{INVENTORY_TABLE}\" WHERE id = '{product_id}'"
    
    try:
        client.query(query)
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
        return jsonify({
            'success': True,
            'products': products,
            'categories': categories,
            'total': len(products)
        })
    except Exception as e:
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
    
    # Merge with existing data
    updated_data = {
        'name': data.get('name', existing['name']),
        'category': data.get('category', existing['category']),
        'quantity': data.get('quantity', existing['quantity']),
        'price': data.get('price', existing['price'])
    }
    
    try:
        product = update_product(product_id, updated_data)
        return jsonify({'success': True, 'product': product})
    except Exception as e:
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
                'error': 'Delete operation not supported. Use truncate instead.'
            }), 501
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health')
def health():
    """Health check endpoint."""
    try:
        workspace_id = get_workspace_id()
        return jsonify({
            'status': 'healthy',
            'workspace_id': workspace_id,
            'table': INVENTORY_TABLE
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500
