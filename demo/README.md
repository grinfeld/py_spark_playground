# Demo Scripts

This directory contains various demonstration scripts showing different aspects of the configuration management system.

## 📁 Available Demos

### **Configuration Management**
- **`demo_explicit_config.py`** - Shows explicit configuration requirements and validation
- **`demo_unified_config.py`** - Demonstrates unified configuration approach
- **`demo_unified_env.py`** - Shows environment variable unification
- **`demo_unified_env_vars.py`** - Demonstrates environment variable precedence

### **Storage Management**
- **`demo_storage_agnostic.py`** - Shows storage-agnostic code patterns
- **`demo_simplified_storage.py`** - Demonstrates simplified storage backend structure
- **`demo_path_style_independent.py`** - Shows independent path style access configuration

### **Catalog Management**
- **`demo_catalog_types.py`** - Demonstrates different catalog types (Hive, Glue, S3)
- **`demo_catalog_inheritance.py`** - Shows catalog inheritance structure
- **`demo_catalog_env_config.py`** - Demonstrates environment-driven catalog configuration
- **`demo_catalog_config_test.py`** - Tests catalog configuration patterns
- **`demo_catalog_type_validation.py`** - Tests catalog type validation and error handling

### **Warehouse & IO Configuration**
- **`demo_simplified_warehouse.py`** - Shows simplified warehouse path configuration
- **`demo_simplified_io_impl.py`** - Demonstrates simplified IO implementation configuration

### **Architecture & Design**
- **`demo_polymorphism.py`** - Shows polymorphic design patterns
- **`demo_inheritance_improvements.py`** - Demonstrates inheritance improvements

## 🚀 Running Demos

### **Run All Demos**
```bash
# From the project root
python demo/run_all_demos.py

# List available demos
python demo/run_all_demos.py --list
```

### **Run Individual Demos**
```bash
# From the project root
python demo/demo_explicit_config.py
python demo/demo_polymorphism.py
python demo/demo_storage_agnostic.py
```

### **Run from Demo Directory**
```bash
# From the demo directory
cd demo
python demo_explicit_config.py
python demo_polymorphism.py
```

## 📋 Demo Categories

### **Configuration Validation**
These demos show how the system validates required environment variables:
- `demo_explicit_config.py` - Required variables validation
- `demo_unified_config.py` - Unified configuration approach

### **Storage Abstraction**
These demos show storage-agnostic patterns:
- `demo_storage_agnostic.py` - No if statements for storage types
- `demo_simplified_storage.py` - Simplified storage backend structure

### **Catalog Management**
These demos show different catalog types and configurations:
- `demo_catalog_types.py` - Hive, Glue, S3 catalogs
- `demo_catalog_inheritance.py` - Inheritance structure
- `demo_catalog_env_config.py` - Environment-driven configuration

### **Architecture Patterns**
These demos show design patterns and improvements:
- `demo_polymorphism.py` - Polymorphic design
- `demo_inheritance_improvements.py` - Inheritance improvements

## 🔧 Demo Features

### **Environment Variable Testing**
Most demos test different environment variable combinations:
- Required vs optional variables
- Default values vs explicit configuration
- Error handling for missing variables

### **Configuration Validation**
Demos show how the system validates:
- Required environment variables
- Configuration consistency
- Error messages for missing variables

### **Storage Flexibility**
Demos demonstrate:
- MinIO vs S3 configuration
- Storage-agnostic code patterns
- Polymorphic storage backends

### **Catalog Flexibility**
Demos show:
- Different catalog types (Hive, Glue, S3)
- Environment-driven configuration
- Inheritance and polymorphism

## 📊 Demo Output

Each demo provides:
- ✅ Success/failure indicators
- 📦 Configuration details
- 🔧 Technical explanations
- 📋 Usage examples
- 🎯 Benefits summary

## 🎯 Learning Path

1. **Start with**: `demo_explicit_config.py` - Understand required configuration
2. **Then**: `demo_storage_agnostic.py` - See storage-agnostic patterns
3. **Next**: `demo_polymorphism.py` - Understand the architecture
4. **Finally**: `demo_catalog_types.py` - See different catalog configurations

## 🔍 Troubleshooting

If demos fail:
1. Check that all required environment variables are set
2. Ensure you're running from the project root directory
3. Verify that `config_manager.py` is in the project root
4. Check Python path and import statements

## 📝 Adding New Demos

To add a new demo:
1. Create `demo_new_feature.py` in this directory
2. Add proper imports and path handling
3. Include clear documentation and examples
4. Test the demo from both root and demo directories
5. Update this README with the new demo description
