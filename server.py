from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import requests
import anthropic
import json
import xml.etree.ElementTree as ET
import re

app = Flask(__name__, static_folder='.')
CORS(app)

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/test', methods=['GET'])
def test():
    return jsonify({'status': 'ok', 'message': 'Server is running!'})

def parse_odata_metadata(xml_content):
    """Parse OData $metadata XML to extract all entities"""
    print("📊 Parsing OData metadata XML...")
    
    try:
        # Remove BOM if present
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
        
        root = ET.fromstring(xml_content)
        
        # OData namespaces (try multiple versions)
        namespaces = {
            'edmx': 'http://schemas.microsoft.com/ado/2007/06/edmx',
            'edm': 'http://schemas.microsoft.com/ado/2008/09/edm',
            'edm2': 'http://schemas.microsoft.com/ado/2009/11/edm',
            'edm3': 'http://docs.oasis-open.org/odata/ns/edm',
            'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
            'sap': 'http://www.sap.com/Protocols/SAPData'
        }
        
        entities = {}
        entity_containers = {}
        
        # First pass: Get all EntityType definitions
        for ns_prefix in ['edm', 'edm2', 'edm3']:
            ns = namespaces.get(ns_prefix, '')
            if not ns:
                continue
                
            for entity_type in root.findall(f'.//{{{ns}}}EntityType'):
                entity_name = entity_type.get('Name')
                if not entity_name:
                    continue
                
                properties = []
                keys = []
                nav_props = []
                
                # Get key properties
                key_elem = entity_type.find(f'{{{ns}}}Key')
                if key_elem is not None:
                    for prop_ref in key_elem.findall(f'{{{ns}}}PropertyRef'):
                        key_name = prop_ref.get('Name')
                        if key_name:
                            keys.append(key_name)
                
                # Get properties
                for prop in entity_type.findall(f'{{{ns}}}Property'):
                    prop_name = prop.get('Name')
                    prop_type = prop.get('Type', '')
                    if prop_name:
                        # Check SAP annotations for filterability, sortability
                        filterable = prop.get('{http://www.sap.com/Protocols/SAPData}filterable', 'true')
                        sortable = prop.get('{http://www.sap.com/Protocols/SAPData}sortable', 'true')
                        
                        properties.append({
                            'name': prop_name,
                            'type': prop_type.split('.')[-1],  # Get just the type name
                            'filterable': filterable.lower() == 'true',
                            'sortable': sortable.lower() == 'true'
                        })
                
                # Get navigation properties
                for nav in entity_type.findall(f'{{{ns}}}NavigationProperty'):
                    nav_name = nav.get('Name')
                    if nav_name:
                        nav_props.append(nav_name)
                
                entities[entity_name] = {
                    'properties': properties[:20],  # Limit to first 20
                    'keys': keys,
                    'navigation_properties': nav_props[:15]
                }
        
        # Second pass: Get all EntitySet definitions (these are the actual endpoints)
        for ns_prefix in ['edm', 'edm2', 'edm3']:
            ns = namespaces.get(ns_prefix, '')
            if not ns:
                continue
            
            for entity_set in root.findall(f'.//{{{ns}}}EntitySet'):
                set_name = entity_set.get('Name')
                entity_type = entity_set.get('EntityType')
                
                if set_name and entity_type:
                    # Extract just the type name (remove namespace)
                    type_name = entity_type.split('.')[-1]
                    
                    if type_name in entities:
                        entity_containers[set_name] = {
                            'entity_type': type_name,
                            'properties': entities[type_name]['properties'],
                            'keys': entities[type_name]['keys'],
                            'navigation_properties': entities[type_name]['navigation_properties']
                        }
        
        print(f"✅ Found {len(entity_containers)} EntitySets")
        for i, name in enumerate(list(entity_containers.keys())[:5]):
            print(f"   {i+1}. {name}")
        
        return entity_containers
        
    except ET.ParseError as e:
        print(f"❌ XML Parse Error: {str(e)}")
        return {}
    except Exception as e:
        print(f"❌ Error parsing metadata: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}

def build_auth_headers(auth_config):
    """Build authentication headers based on config"""
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'DataServiceVersion': '2.0',  # SAP OData requirement
        'MaxDataServiceVersion': '3.0'
    }
    
    if not auth_config or auth_config.get('type') == 'none':
        return headers
    
    auth_type = auth_config.get('type')
    
    if auth_type == 'apikey':
        api_key = auth_config.get('api_key', '')
        if api_key:
            headers['APIKey'] = api_key
    
    elif auth_type == 'basic':
        username = auth_config.get('username', '')
        password = auth_config.get('password', '')
        if username and password:
            import base64
            credentials = f"{username}:{password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers['Authorization'] = f'Basic {encoded}'
    
    elif auth_type == 'bearer':
        token = auth_config.get('token', '')
        if token:
            headers['Authorization'] = f'Bearer {token}'
    
    return headers

@app.route('/api/fetch-metadata', methods=['POST'])
def fetch_metadata():
    """Fetch and parse OData $metadata XML"""
    print("\n" + "="*60)
    print("📥 FETCH METADATA REQUEST")
    print("="*60)
    
    try:
        data = request.json
        metadata_url = data.get('metadata_url', '').strip()
        service_url = data.get('service_url', '').strip()
        auth_config = data.get('auth_config', {})
        
        # Determine the metadata URL
        if metadata_url:
            url = metadata_url
        elif service_url:
            # Construct metadata URL from service URL
            url = service_url.rstrip('/') + '/$metadata'
        else:
            return jsonify({'error': 'Either metadata_url or service_url is required'}), 400
        
        print(f"🔍 Fetching: {url}")
        print(f"🔐 Auth type: {auth_config.get('type', 'none')}")
        
        # Prepare headers with authentication
        # For $metadata, we need to accept XML, not JSON
        headers = build_auth_headers(auth_config)
        headers['Accept'] = 'application/xml, text/xml, application/atom+xml'  # Override for metadata
        headers.pop('DataServiceVersion', None)  # Not needed for metadata
        headers.pop('MaxDataServiceVersion', None)  # Not needed for metadata
        
        print(f"📋 Request headers: Accept: {headers.get('Accept')}")
        
        # Fetch the metadata
        response = requests.get(url, headers=headers, timeout=30)
        
        print(f"📊 Status: {response.status_code}")
        print(f"📊 Content-Type: {response.headers.get('content-type', 'unknown')}")
        
        if response.status_code == 401:
            return jsonify({
                'error': 'Authentication required. Please provide an API key.',
                'status': 401
            }), 401
        
        if response.status_code != 200:
            return jsonify({
                'error': f'Failed to fetch metadata: HTTP {response.status_code}',
                'details': response.text[:500]
            }), 500
        
        # Check if it's actually XML
        content_type = response.headers.get('content-type', '').lower()
        if 'xml' not in content_type and not response.text.strip().startswith('<?xml'):
            return jsonify({
                'error': 'Response is not XML. The URL might be incorrect.',
                'content_type': content_type,
                'preview': response.text[:500]
            }), 500
        
        # Parse the metadata
        xml_content = response.text
        entities = parse_odata_metadata(xml_content)
        
        if not entities:
            return jsonify({
                'error': 'No entities found in metadata',
                'xml_preview': xml_content[:1000]
            }), 500
        
        print(f"✅ Successfully parsed {len(entities)} entities")
        print("="*60 + "\n")
        
        return jsonify({
            'success': True,
            'entities': entities,
            'entity_count': len(entities)
        })
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {str(e)}")
        return jsonify({'error': f'Network error: {str(e)}'}), 500
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/manual-entities', methods=['POST'])
def manual_entities():
    """Accept manually provided entity names"""
    print("\n" + "="*60)
    print("📝 MANUAL ENTITIES INPUT")
    print("="*60)
    
    try:
        data = request.json
        entity_names = data.get('entity_names', [])
        
        if not entity_names:
            return jsonify({'error': 'No entity names provided'}), 400
        
        print(f"📦 Received {len(entity_names)} entities:")
        for name in entity_names[:10]:
            print(f"   • {name}")
        
        # Create basic entity structure
        entities = {}
        for name in entity_names:
            entities[name] = {
                'entity_type': name,
                'properties': [],
                'keys': [],
                'navigation_properties': []
            }
        
        print("✅ Entities processed")
        print("="*60 + "\n")
        
        return jsonify({
            'success': True,
            'entities': entities,
            'entity_count': len(entities)
        })
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/suggest-personas', methods=['POST'])
def suggest_personas():
    """Suggest relevant personas based on discovered entities"""
    print("\n" + "="*60)
    print("👥 SUGGEST PERSONAS")
    print("="*60)
    
    try:
        data = request.json
        entities = data.get('entities', {})
        api_key = data.get('api_key')
        
        if not entities or not api_key:
            return jsonify({'error': 'Entities and API key required'}), 400
        
        entity_list = list(entities.keys())[:10]
        print(f"📦 Analyzing {len(entity_list)} entities...")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        # Retry logic for API overload
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                message = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=2048,
                    messages=[{
                        "role": "user",
                        "content": f"""Based on these OData entities, suggest 5-7 relevant user personas who would interact with this API.

Entities: {', '.join(entity_list)}

For each persona, provide:
1. Role/title (e.g., "Sales Manager", "Customer Service Rep", "Data Analyst")
2. Brief description of their needs and how they'd use the API
3. Their technical skill level (beginner/intermediate/advanced)
4. Query style (casual/business/technical)

Return ONLY valid JSON:
{{
  "personas": [
    {{
      "id": "sales_manager",
      "title": "Sales Manager",
      "description": "Needs to track sales performance, review orders, and monitor customer accounts",
      "skill_level": "beginner",
      "query_style": "business",
      "example_queries": ["Show me today's sales", "Which customers ordered the most this month"]
    }}
  ]
}}"""
                    }]
                )
                
                # Success - break retry loop
                break
                
            except anthropic.APIError as e:
                if any(keyword in str(e).lower() for keyword in ['overload', 'rate', '529', '503', '502']):
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        print(f"⚠️ API overloaded (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s...")
                        import time
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"API overloaded after {max_retries} attempts. Please wait and try again.") from e
                else:
                    raise
        
        response_text = message.content[0].text
        
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            response_text = response_text.split('```')[1].split('```')[0].strip()
        
        personas = json.loads(response_text)
        print(f"✅ Generated {len(personas.get('personas', []))} personas")
        print("="*60 + "\n")
        
        return jsonify({
            'success': True,
            'personas': personas.get('personas', [])
        })
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/sample-data', methods=['POST'])
def fetch_sample_data():
    """Fetch sample data from entities to get real values"""
    print("\n" + "="*60)
    print("📊 FETCH SAMPLE DATA")
    print("="*60)
    
    try:
        data = request.json
        service_url = data.get('service_url', '').strip()
        entities = data.get('entities', {})
        auth_config = data.get('auth_config', {})
        sample_size = data.get('sample_size', 5)
        
        if not service_url or not entities:
            return jsonify({'error': 'Service URL and entities required'}), 400
        
        print(f"🔍 Fetching sample data from {len(entities)} entities...")
        print(f"📦 Sample size: {sample_size} records per entity")
        
        headers = build_auth_headers(auth_config)
        
        entity_samples = {}
        
        for entity_name in list(entities.keys())[:10]:  # Limit to first 10 entities
            try:
                # Construct URL with proper OData parameters
                # Add $inlinecount for SAP compatibility
                url = f"{service_url.rstrip('/')}/{entity_name}?$top={sample_size}&$inlinecount=allpages&$format=json"
                
                print(f"  📥 Fetching: {entity_name}")
                print(f"     URL: {url}")
                
                response = requests.get(url, headers=headers, timeout=15)
                
                print(f"     Status: {response.status_code}")
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '')
                    print(f"     Content-Type: {content_type}")
                    
                    json_data = response.json()
                    
                    # Extract records
                    records = []
                    if isinstance(json_data, dict):
                        if 'd' in json_data and 'results' in json_data['d']:
                            records = json_data['d']['results']
                        elif 'value' in json_data:
                            records = json_data['value']
                        elif 'd' in json_data and isinstance(json_data['d'], list):
                            records = json_data['d']
                        elif 'd' in json_data and not isinstance(json_data['d'], list):
                            # Single record wrapped in 'd'
                            records = [json_data['d']]
                    
                    if records:
                        # Extract unique values for each property
                        property_values = {}
                        
                        for record in records:
                            for key, value in record.items():
                                # Skip metadata and complex objects
                                if key.startswith('__') or isinstance(value, dict) or isinstance(value, list):
                                    continue
                                
                                if key not in property_values:
                                    property_values[key] = set()
                                
                                # Add value if it's simple type and not null
                                if value is not None and not isinstance(value, (dict, list)):
                                    # Store the actual value type for better filter generation
                                    str_value = str(value)
                                    
                                    # For booleans, normalize to lowercase
                                    if isinstance(value, bool):
                                        str_value = 'true' if value else 'false'
                                    # For strings that look like booleans, keep as-is but add note
                                    elif str_value.lower() in ['true', 'false']:
                                        str_value = f"{str_value} (boolean)"
                                    
                                    property_values[key].add(str_value)
                        
                        # Convert sets to lists and limit
                        sample_values = {}
                        for key, values in property_values.items():
                            sample_values[key] = list(values)[:5]  # Keep max 5 unique values per property
                        
                        entity_samples[entity_name] = {
                            'record_count': len(records),
                            'sample_values': sample_values,
                            'sample_record': records[0] if records else None
                        }
                        
                        print(f"    ✅ Got {len(records)} records, {len(sample_values)} properties")
                    else:
                        print(f"    ⚠️ No records returned (empty result set)")
                        entity_samples[entity_name] = {'record_count': 0, 'sample_values': {}}
                        
                elif response.status_code == 401:
                    print(f"    ❌ HTTP 401 - Authentication required or invalid API key")
                    entity_samples[entity_name] = {'error': 'Authentication failed - check API key'}
                elif response.status_code == 403:
                    print(f"    ❌ HTTP 403 - Forbidden (insufficient permissions)")
                    entity_samples[entity_name] = {'error': 'Access forbidden'}
                elif response.status_code == 404:
                    print(f"    ❌ HTTP 404 - Entity not found")
                    entity_samples[entity_name] = {'error': 'Entity not found'}
                else:
                    error_body = response.text[:200] if response.text else 'No error message'
                    print(f"    ❌ HTTP {response.status_code}: {error_body}")
                    entity_samples[entity_name] = {'error': f'HTTP {response.status_code}'}
                    
            except requests.exceptions.Timeout:
                print(f"    ⏱️ Timeout after 15 seconds")
                entity_samples[entity_name] = {'error': 'Request timeout'}
            except requests.exceptions.ConnectionError as e:
                print(f"    ❌ Connection error: {str(e)[:100]}")
                entity_samples[entity_name] = {'error': 'Connection failed'}
            except json.JSONDecodeError as e:
                print(f"    ❌ Invalid JSON response: {str(e)}")
                entity_samples[entity_name] = {'error': 'Invalid JSON response'}
            except Exception as e:
                print(f"    ❌ Error: {str(e)[:100]}")
                entity_samples[entity_name] = {'error': str(e)[:200]}
        
        print(f"✅ Fetched sample data from {len(entity_samples)} entities")
        print("="*60 + "\n")
        
        return jsonify({
            'success': True,
            'samples': entity_samples
        })
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/generate-utterances', methods=['POST'])
def generate_utterances():
    """Generate utterances for an entity with optional persona"""
    print("\n" + "="*60)
    print("✨ GENERATE UTTERANCES")
    print("="*60)
    
    try:
        data = request.json
        entity = data.get('entity')
        entity_info = data.get('entity_info', {})
        api_key = data.get('api_key')
        count = data.get('count', 10)
        persona = data.get('persona', None)
        sample_data = data.get('sample_data', None)  # NEW: Real values from API
        
        print(f"📝 Entity: {entity}")
        print(f"📊 Count: {count}")
        if persona:
            print(f"👤 Persona: {persona.get('title', 'Unknown')}")
        if sample_data:
            print(f"✨ Using real sample data from API")
        
        if not entity or not api_key:
            return jsonify({'error': 'Entity and API key are required'}), 400
        
        # Get property names
        properties = entity_info.get('properties', [])
        prop_names = [p['name'] if isinstance(p, dict) else p for p in properties]
        
        client = anthropic.Anthropic(api_key=api_key)
        
        # Build persona-specific prompt
        if persona:
            persona_context = f"""
You are generating queries from the perspective of a {persona.get('title', 'user')}:
- Description: {persona.get('description', '')}
- Skill Level: {persona.get('skill_level', 'intermediate')}
- Query Style: {persona.get('query_style', 'business')}

Generate queries that this persona would naturally ask in their daily work.
Use {persona.get('query_style', 'business')} language appropriate for their skill level.

Example queries this persona might ask:
{chr(10).join('- ' + q for q in persona.get('example_queries', [])[:3])}

Make the utterances sound natural and conversational, reflecting how {persona.get('title', 'this user')} would actually speak."""
        else:
            persona_context = "Generate natural language queries that various users might ask."
        
        # Build property info with data types
        property_details = []
        filterable_props = []
        non_filterable_props = []
        sortable_props = []
        
        for prop in properties[:20]:
            if isinstance(prop, dict):
                prop_name = prop.get('name', '')
                prop_type = prop.get('type', 'String')
                is_filterable = prop.get('filterable', True)
                is_sortable = prop.get('sortable', True)
                
                # Track filterable vs non-filterable
                if is_filterable:
                    filterable_props.append(f"{prop_name} ({prop_type})")
                else:
                    non_filterable_props.append(prop_name)
                
                if is_sortable:
                    sortable_props.append(prop_name)
                
                # Add to full list
                filter_note = "" if is_filterable else " [NOT FILTERABLE]"
                property_details.append(f"{prop_name} ({prop_type}){filter_note}")
            else:
                property_details.append(str(prop))
        
        property_info = ', '.join(property_details)
        
        print(f"  🔒 Filterable properties: {filterable_props[:5]}")
        print(f"  🚫 Non-filterable properties: {non_filterable_props[:5]}")
        
        # Group properties by type for better filter suggestions
        string_props = [p.get('name') if isinstance(p, dict) else p for p in properties if isinstance(p, dict) and 'String' in p.get('type', '')]
        numeric_props = [p.get('name') if isinstance(p, dict) else p for p in properties if isinstance(p, dict) and any(t in p.get('type', '') for t in ['Int', 'Decimal', 'Double', 'Float'])]
        date_props = [p.get('name') if isinstance(p, dict) else p for p in properties if isinstance(p, dict) and 'Date' in p.get('type', '')]
        bool_props = [p.get('name') if isinstance(p, dict) else p for p in properties if isinstance(p, dict) and 'Boolean' in p.get('type', '')]
        
        # Build sample values context
        sample_values_context = ""
        has_sample_data = False
        
        if sample_data and 'sample_values' in sample_data and sample_data['sample_values']:
            has_sample_data = True
            sample_values_context = "\n\n🎯 REAL VALUES FROM API DATABASE (MANDATORY TO USE):\n"
            sample_values_context += "="*60 + "\n"
            
            for prop, values in list(sample_data['sample_values'].items())[:25]:  # Show more properties
                if values and len(values) > 0:
                    # Format values nicely
                    formatted_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values[:8]])
                    sample_values_context += f"• {prop}: {formatted_values}\n"
            
            sample_values_context += "="*60 + "\n"
            sample_values_context += f"""
🚨 CRITICAL INSTRUCTIONS FOR USING SAMPLE DATA:

1. **ONLY use values listed above** in your filter conditions
2. **DO NOT invent or guess values** - stick to what's shown
3. **Match EXACT casing** - 'Active' ≠ 'ACTIVE' ≠ 'active'
4. **Use values that will return results** - all values above exist in the database

GOOD EXAMPLES (using real values):
- If Country shows ['DE', 'US', 'GB']: Use $filter=Country eq 'DE'  ✅
- If Status shows ['ACTIVE', 'CLOSED']: Use $filter=Status eq 'ACTIVE'  ✅
- If Price shows ['99.99', '149.50', '299.00']: Use $filter=Price eq 99.99  ✅

BAD EXAMPLES (inventing values):
- $filter=Country eq 'France'  ❌ (not in the list - will return 0 results)
- $filter=Status eq 'Pending'  ❌ (not in the list - will return 0 results)
- $filter=Price eq 999.99  ❌ (not in the list - will return 0 results)

📊 Sample record count: {sample_data.get('record_count', 0)} records available
✅ All filters using these values are GUARANTEED to return results
"""
        else:
            sample_values_context = """
⚠️ WARNING: No sample data available. Generating generic queries.
- These queries may not match your actual data
- Validation may fail if values don't exist in database
- For best results, fetch sample data first using the "📊 Fetch Sample Data" button
"""
        
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": f"""Generate {count} natural, conversational utterances for querying the {entity} OData entity.

{persona_context}

Entity: {entity}
Key Fields: {entity_info.get('keys', [])}
Sample Properties: {prop_names[:15]}
Navigation Properties: {entity_info.get('navigation_properties', [])}

{sample_values_context}

Create diverse queries with varying complexity:

SIMPLE (40% - everyday queries):
- Natural, conversational questions
- Basic data retrieval without filters (to ensure results)
- Simple filters **ONLY using the exact values from the REAL VALUES list above**
Examples: 
  • "Show me all {entity}" → /{entity}?$top=20
  • "Get the first 10 {entity}" → /{entity}?$top=10
  • If Status=['ACTIVE']: "Show active records" → $filter=Status eq 'ACTIVE'

MEDIUM (40% - specific business queries):
- Filtering by specific criteria
- Sorting and organizing results
- Selecting relevant fields
Examples: "Show customers in New York sorted by name", "Get my top 10 orders by value"

COMPLEX (20% - advanced analysis):
- Multiple conditions
- Data relationships
- Aggregations
Examples: "Show customers who ordered more than 5 times and include their addresses", "Get high-value orders with customer details"

IMPORTANT: 
- Use natural, conversational language
- Avoid technical jargon unless the persona is technical
- Make queries sound like real user requests
- Include common business scenarios

Return ONLY valid JSON array:
[
  {{
    "utterance": "Natural conversational query",
    "suggested_endpoint": "/{entity}?$filter=...",
    "complexity": "simple",
    "operations_used": ["GET", "$filter"]
  }}
]"""
            }]
        )
        
        utterances_text = message.content[0].text
        
        # Extract JSON from markdown if needed
        if '```json' in utterances_text:
            utterances_text = utterances_text.split('```json')[1].split('```')[0].strip()
        elif '```' in utterances_text:
            utterances_text = utterances_text.split('```')[1].split('```')[0].strip()
        
        utterances = json.loads(utterances_text)
        
        # POST-PROCESS: Ensure every endpoint has $top to prevent buffer overflow
        # AND remove non-filterable properties from $filter clauses
        
        # Get list of non-filterable properties for validation
        non_filterable = []
        for prop in properties:
            if isinstance(prop, dict) and not prop.get('filterable', True):
                non_filterable.append(prop.get('name', ''))
        
        for utterance in utterances:
            endpoint = utterance.get('suggested_endpoint', '')
            original_endpoint = endpoint
            modified = False
            
            # Check if $top is missing
            if endpoint and '$top' not in endpoint.lower():
                # Add $top parameter
                if '?' in endpoint:
                    endpoint = f"{endpoint}&$top=50"
                else:
                    endpoint = f"{endpoint}?$top=50"
                modified = True
                print(f"  ⚙️ Auto-added $top=50")
            
            # Check for non-filterable properties in $filter
            if '$filter' in endpoint.lower() and non_filterable:
                import re
                # Extract the $filter clause
                filter_match = re.search(r'\$filter=([^&]+)', endpoint, re.IGNORECASE)
                if filter_match:
                    filter_clause = filter_match.group(1)
                    
                    # Check if any non-filterable properties are used
                    used_non_filterable = []
                    for prop in non_filterable:
                        # Case-insensitive check for property name in filter
                        if re.search(r'\b' + re.escape(prop) + r'\b', filter_clause, re.IGNORECASE):
                            used_non_filterable.append(prop)
                    
                    if used_non_filterable:
                        # Remove the entire $filter clause to prevent errors
                        print(f"  ⚠️ Removing $filter with non-filterable properties: {', '.join(used_non_filterable)}")
                        # Remove $filter and its value
                        endpoint = re.sub(r'[&?]\$filter=[^&]*', '', endpoint, flags=re.IGNORECASE)
                        # Clean up double & or ? at start
                        endpoint = re.sub(r'\?&', '?', endpoint)
                        endpoint = re.sub(r'&&', '&', endpoint)
                        modified = True
                        utterance['removed_non_filterable'] = used_non_filterable
                        utterance['warning'] = f"Original filter removed due to non-filterable properties: {', '.join(used_non_filterable)}"
            
            if modified:
                utterance['suggested_endpoint'] = endpoint
                if original_endpoint != endpoint:
                    utterance['original_endpoint'] = original_endpoint
                    utterance['auto_modified'] = True
        
        # Add persona info to each utterance
        if persona:
            for u in utterances:
                u['persona'] = persona.get('title')
                u['persona_id'] = persona.get('id')
        
        print(f"✅ Generated {len(utterances)} utterances")
        print("="*60 + "\n")
        
        return jsonify({
            'success': True,
            'utterances': utterances
        })
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON error: {str(e)}")
        return jsonify({'error': f'Failed to parse response: {str(e)}', 'raw': utterances_text[:500]}), 500
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/validate-endpoint', methods=['POST'])
def validate_endpoint():
    """Validate a single endpoint against the OData service"""
    print("\n" + "="*60)
    print("✅ VALIDATE ENDPOINT")
    print("="*60)
    
    try:
        data = request.json
        service_url = data.get('service_url', '').strip()
        endpoint = data.get('endpoint', '').strip()
        auth_config = data.get('auth_config', {})
        
        if not service_url or not endpoint:
            return jsonify({'error': 'Service URL and endpoint required'}), 400
        
        # Construct full URL
        full_url = service_url.rstrip('/') + endpoint
        
        # Add $format=json if not already present
        if '?' in endpoint:
            full_url += '&$format=json'
        else:
            full_url += '?$format=json'
        
        print(f"🔍 Testing: {full_url}")
        print(f"🔐 Auth type: {auth_config.get('type', 'none')}")
        
        # Prepare headers with authentication
        headers = build_auth_headers(auth_config)
        
        print(f"📋 Headers: {', '.join([f'{k}: {v[:20]}...' if len(str(v)) > 20 else f'{k}: {v}' for k, v in headers.items()])}")
        
        # Make request
        response = requests.get(full_url, headers=headers, timeout=15)
        
        print(f"📊 Response Status: {response.status_code}")
        print(f"📊 Response Content-Type: {response.headers.get('content-type', 'unknown')}")
        print(f"📊 Response Size: {len(response.text)} chars")
        
        success = response.status_code == 200
        
        result = {
            'success': success,
            'status_code': response.status_code,
            'url': full_url
        }
        
        if success:
            result['message'] = 'Endpoint is valid!'
            try:
                json_data = response.json()
                result['sample_data'] = json_data
                
                # Extract useful info for display
                if isinstance(json_data, dict):
                    # Count results if it's an OData response
                    if 'd' in json_data:
                        if 'results' in json_data['d'] and isinstance(json_data['d']['results'], list):
                            result['result_count'] = len(json_data['d']['results'])
                            if json_data['d']['results']:
                                result['preview'] = json_data['d']['results'][0]
                        elif isinstance(json_data['d'], list):
                            result['result_count'] = len(json_data['d'])
                            if json_data['d']:
                                result['preview'] = json_data['d'][0]
                        elif not isinstance(json_data['d'], list):
                            # Single record
                            result['result_count'] = 1
                            result['preview'] = json_data['d']
                        
                        # Check for inline count
                        if '__count' in json_data['d']:
                            result['total_count'] = json_data['d']['__count']
                    elif 'value' in json_data:
                        result['result_count'] = len(json_data['value'])
                        if json_data['value']:
                            result['preview'] = json_data['value'][0]
                        # OData v4 count
                        if '@odata.count' in json_data:
                            result['total_count'] = json_data['@odata.count']
                    
            except json.JSONDecodeError:
                result['sample_data'] = response.text[:1000]
        else:
            result['message'] = response.text[:500]
            result['error_details'] = response.text[:2000]
        
        print(f"{'✅' if success else '❌'} Result: {result.get('message', 'Unknown')[:100]}")
        if success and 'result_count' in result:
            print(f"📊 Returned {result['result_count']} record(s)")
            if 'total_count' in result:
                print(f"📊 Total available: {result['total_count']} record(s)")
        print("="*60 + "\n")
        
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        print("⏱️ Timeout")
        return jsonify({'success': False, 'error': 'Request timeout'}), 500
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🚀 SAP OData Utterance Generator - Metadata-First Approach")
    print("="*70)
    print("\n✨ This version:")
    print("  • Fetches $metadata XML directly (most reliable)")
    print("  • Parses EntitySets, properties, and relationships")
    print("  • Supports manual entity input as fallback")
    print("  • Manual validation to control API usage")
    print("\n📍 Server: http://localhost:5000")
    print("="*70 + "\n")
    
    app.run(debug=True, port=5000, host='0.0.0.0')