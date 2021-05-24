package org.xbasej.annotations.processor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.JavaFileObject;

import org.xBaseJ.annotations.DBFField;

import com.google.auto.service.AutoService;

@SupportedAnnotationTypes({ "org.xBaseJ.annotations.DBFField" })
@SupportedSourceVersion(SourceVersion.RELEASE_11)
@AutoService(Processor.class)
public class DBFFieldProcessor extends AbstractProcessor {

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		if (annotations.isEmpty()) {
			return false;
		}
		Map<Element, List<Element>> annotatedElements = new LinkedHashMap<>();
		for (TypeElement annotation : annotations) {
			for (Element e : roundEnv.getElementsAnnotatedWith(annotation)) {
				Element parent = e.getEnclosingElement();
				if (!annotatedElements.containsKey(parent)) {
					annotatedElements.put(parent, new ArrayList<>());
				}
				annotatedElements.get(parent).add(e);
			}
		}
		for (Element parent : annotatedElements.keySet()) {
			String className = parent.asType().toString();
			try {
				writeImplFile(className, annotatedElements.get(parent));
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		return true;
	}

	private void writeImplFile(final String className, List<Element> elements) throws IOException {

		String packageName = null;
		int lastDot = className.lastIndexOf('.');
		if (lastDot > 0) {
			packageName = className.substring(0, lastDot);
		}

		generateDbfCode(className, elements, packageName, lastDot);
	}

	private void generateDbfCode(final String className, List<Element> elements, String packageName, int lastDot)
			throws IOException {
		String dbfRecordClassName;
		if (className.toLowerCase().endsWith("dbfstruct")) {
			dbfRecordClassName = className.substring(0, className.length() - "dbfstruct".length());
		} else if (className.toLowerCase().endsWith("struct")) {
			dbfRecordClassName = className.substring(0, className.length() - "struct".length());
		} else if (className.toLowerCase().endsWith("dbf")) {
			dbfRecordClassName = className.substring(0, className.length() - "dbf".length());
		} else if (className.toLowerCase().endsWith("dbffieldset")) {
			dbfRecordClassName = className.substring(0, className.length() - "dbffieldset".length());
		} else {
			dbfRecordClassName = className + "DBFRecord";
		}
		String dbfRecordSimpleClassName = dbfRecordClassName.substring(lastDot + 1);

		final Filer filer = processingEnv.getFiler();
		JavaFileObject builderFile = filer.createSourceFile(dbfRecordClassName);

		try (PrintWriter out = new PrintWriter(builderFile.openWriter())) {
			if (packageName != null) {
				out.print("package ");
				out.print(packageName);
				out.println(";");
				out.println();
			}

			out.print("public class ");
			out.print(dbfRecordSimpleClassName);
			out.println(" extends " + className);
			out.println(" implements java.lang.Iterable<" + dbfRecordSimpleClassName + ">");
			out.println(" {");
			out.println();

			out.println(" private org.xBaseJ.DBF _dbf;");
			out.println();
			out.println(" public " + dbfRecordSimpleClassName + "(org.xBaseJ.DBF dbf) throws IOException, xBaseJException {");
			out.println("  this(dbf, false);");
			out.println(" }");
			out.println();
			
			out.println(" public " + dbfRecordSimpleClassName + "(org.xBaseJ.DBF dbf, boolean attachOnly) throws IOException, xBaseJException {");
			out.println("  if (dbf!=null && attachOnly) attach(dbf);");
			out.println("  if (dbf!=null && !attachOnly) addFields(dbf);");
			out.println(" }");
			out.println();
			
			out.println(" public " + dbfRecordSimpleClassName + "() {");
			out.println("  _dbf = null;");
			out.println(" }");
			out.println();

			out.println(" public void attach(org.xBaseJ.DBF dbf) {");
			out.println("  _dbf=dbf;");
			out.println("  try {");
			out.println("   init();");
			out.println("  } catch (java.lang.Exception e) {");
			out.println("   throw new java.lang.IllegalStateException(e);");
			out.println("  }");
			out.println(" }");
			out.println();

			out.println(" private void init() throws java.lang.Exception {");
			for (Element element : elements) {
				DBFField a = element.getAnnotation(DBFField.class);
				final String dbfFieldName = a.name().toUpperCase();
				if (!dbfFieldName.matches("(?i)^[a-z_][a-z_0-9]*$")) {
					this.processingEnv.getMessager().printMessage(Kind.ERROR, "Invalid field name: " + a.toString(),
							element);
					return;
				}

				String fieldType = element.asType().toString();
				out.println("  try {");
				out.println("  this." + element.toString() + "=(" + fieldType + ")_dbf.getField(\"" + dbfFieldName
						+ "\");");
				out.println("  } catch (java.lang.Exception e) {this." + element.toString() + "=null;}");
			}
			out.println(" }");
			out.println();
			for (Element element : elements) {
				DBFField a = element.getAnnotation(DBFField.class);
				String fieldName = element.getSimpleName().toString();
				String fieldType = element.asType().toString();
				out.println(" // " + fieldType);
				if (fieldType.endsWith("PictureField")) {
					out.println(" /** " + fieldType + " */");
					out.println(" public byte[] get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return null;");
					out.println("  return this." + fieldName + ".getBytes();");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(byte[] value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("   this." + fieldName + ".put(value);");
					out.println(" }");
				} else if (fieldType.endsWith("LogicalField")) {
					out.println(" /** " + fieldType + " */");
					out.println(" public java.lang.Boolean get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return null;");
					out.println("  if (this." + fieldName + ".get().trim().isEmpty()) return null;");
					out.println("  return this." + fieldName + ".getBoolean();");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(java.lang.Boolean value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  if (value==null) { this." + fieldName + ".put(\"\"); }");
					out.println("  else {");
					out.println("   this." + fieldName + ".put(value);}");
					out.println(" }");
				} else if (fieldType.endsWith("FloatField")) {
					out.println(" /** " + fieldType + " */");
					out.println(" public java.lang.Double get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return null;");
					out.println(" return this." + fieldName + ".getDouble();");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(java.lang.Double value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  if (value==null) { this." + fieldName + ".put(\"\"); }");
					out.println("  else {");
					out.println("   this." + fieldName + ".put(value);}");
					out.println(" }");
				} else if (fieldType.endsWith("CurrencyField")) {
					out.println(" /** " + fieldType + " */");
					out.println(" public java.math.BigDecimal get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return null;");
					out.println("  return this." + fieldName + ".getBigDecimal();");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(java.math.BigDecimal value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  if (value==null) { this." + fieldName + ".put(\"\"); }");
					out.println("  else {");
					out.println("   this." + fieldName + ".put(value);}");
					out.println(" }");
				} else if (fieldType.endsWith("DateField")) {
					out.println(" /** " + fieldType + " */");
					out.println(" public java.time.LocalDate get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return null;");
					out.println("  try { return java.time.LocalDate.parse(this." + fieldName
							+ ".get(),java.time.format.DateTimeFormatter.BASIC_ISO_DATE); }");
					out.println(" catch(java.time.format.DateTimeParseException e) { return null; }");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(java.time.LocalDate value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  if (value==null) { this." + fieldName + ".put(\"\"); }");
					out.println("  else {");
					out.println("   this." + fieldName
							+ ".put(value.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE));}");
					out.println(" }");
				} else if (fieldType.endsWith("NumField") && a.dec() == 0 && a.size() < 10) {
					out.println(" /** " + fieldType + " */");
					out.println(" public int get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return 0;");
					out.println("  try { return java.lang.Integer.parseInt(this." + fieldName + ".get().trim()); }");
					out.println(" catch(java.lang.NumberFormatException e) { return 0; }");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(int value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  this." + fieldName + ".put(java.lang.Integer.toString(value));");
					out.println(" }");
				} else if (fieldType.endsWith("NumField") && a.dec() == 0 && a.size() < 19) {
					out.println(" /** " + fieldType + " */");
					out.println(" public long get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return 0;");
					out.println("  try { return java.lang.Long.parseLong(this." + fieldName + ".get().trim()); }");
					out.println(" catch(java.lang.NumberFormatException e) { return 0; }");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(long value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  this." + fieldName + ".put(java.lang.Long.toString(value));");
					out.println(" }");
					// "".stripTrailing()
				} else if (fieldType.endsWith("CharField")) {
					out.println(" /** " + fieldType + " <br>");
					out.println(" rtrim=" + a.rtrim() + ", ltrim=" + a.ltrim()+" */");
					out.println(" public String get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return \"\";");
					if (a.rtrim() && a.ltrim()) {
						out.println("  return this." + fieldName + ".get().strip();");
					} else if (a.rtrim()) {
						out.println("  return this." + fieldName + ".get().stripTrailing();");
					} else if (a.ltrim()) {
						out.println("  return this." + fieldName + ".get().stripLeading();");
					} else {
						out.println("  return this." + fieldName + ".get();");
					}
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " <br>");
					out.println(" truncate=" + a.truncate()+" */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(String value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					if (a.truncate()) {
						out.println();
						out.println("  if (value!=null && this." + fieldName + ".getMapper()!=null) {");
						out.println("    value=this."+fieldName+".getMapper().map(value);");
						out.println("   }");
						out.println("   if (value!=null && value.length()>"+a.size()+") {");
						out.println("     value = value.substring(0,"+a.size()+");");
						out.println("   }");
						out.println();
					}
					out.println("  this." + fieldName + ".put(value);");
					out.println(" }");
				} else {
					out.println(" /** " + fieldType + " */");
					out.println(" public String get" + methodSubname(fieldName) + "() {");
					out.println("  if (this." + fieldName + "==null) return \"\";");
					out.println("  return this." + fieldName + ".get();");
					out.println(" }");
					out.println();
					out.println(" /** " + fieldType + " */");
					out.println(" public void set" + methodSubname(fieldName)
							+ "(String value) throws org.xBaseJ.xBaseJException {");
					out.println("  if (this." + fieldName + "==null) return;");
					out.println("  this." + fieldName + ".put(value);");
					out.println(" }");
				}

				out.println();
			}

			out.println(
					" public void pack() throws java.lang.CloneNotSupportedException, org.xBaseJ.xBaseJException, java.io.IOException { _dbf.pack(); }");
			out.println(" public boolean deleted() { return _dbf.deleted(); }");
			out.println(
					" public void delete(boolean delete) throws org.xBaseJ.xBaseJException, java.io.IOException { if (delete) _dbf.delete(); else _dbf.undelete(); }");
			out.println(
					" public void seek(int recno) throws org.xBaseJ.xBaseJException, java.io.IOException { _dbf.gotoRecord(recno); }");
			out.println(
					" public void append() throws org.xBaseJ.xBaseJException, java.io.IOException { _dbf.write(); }");
			out.println(
					" public void update() throws org.xBaseJ.xBaseJException, java.io.IOException { _dbf.update(); }");
			out.println(" public void blank() throws org.xBaseJ.xBaseJException {");
			for (Element element : elements) {
				String fieldName = element.getSimpleName().toString();
				String fieldType = element.asType().toString();
				DBFField a = element.getAnnotation(DBFField.class);
				if (fieldType.endsWith("MemoField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("PictureField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("LogicalField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("FloatField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("CurrencyField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("DateField")) {
					out.println("  set" + methodSubname(fieldName) + "(null);");
				} else if (fieldType.endsWith("NumField") && a.dec() == 0 && a.size() < 19) {
					out.println("  set" + methodSubname(fieldName) + "(0);");
				} else {
					out.println("  set" + methodSubname(fieldName) + "(\"\");");
				}
			}
			out.println(" }");
			out.println();

			out.println(" public void appendBlank() throws org.xBaseJ.xBaseJException, java.io.IOException {");
			out.println("  blank();");
			out.println("  append();");
			out.println(" }");
			out.println();

			out.println(" public java.util.List<org.xBaseJ.fields.Field> fields() {");
			out.println("  java.util.List<org.xBaseJ.fields.Field> tmp = new java.util.ArrayList<>(" + elements.size()
					+ ");");
			for (Element element : elements) {
				String fieldName = element.getSimpleName().toString();
				out.println("  if (" + fieldName + "!=null) tmp.add(" + fieldName + ");");
			}
			out.println("  return tmp;");
			out.println(" }");
			out.println();

			out.println(
					" public void addFieldsTo(org.xBaseJ.DBF dbf) throws java.io.IOException, org.xBaseJ.xBaseJException {");
			out.println("  attach(dbf); //attach already existing fields");
			out.println("  java.util.List<org.xBaseJ.fields.Field> list = new java.util.ArrayList<>();");

			for (Element element : elements) {
				DBFField a = element.getAnnotation(DBFField.class);

				final String dbfFieldName = a.name();
				final int size = a.size();
				final int dec = a.dec();

				String fieldName = element.getSimpleName().toString();
				String fieldType = element.asType().toString();

				if (fieldType.endsWith("CharField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"," + size + "); list.add(" + fieldName
							+ ");}");
				} else
				if (fieldType.endsWith("DateField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"); list.add(" + fieldName + ");}");
				} else
				if (fieldType.endsWith("FloatField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"," + size + "," + dec + "); list.add("
							+ fieldName + ");}");
				} else
				if (fieldType.endsWith("LogicalField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"); list.add(" + fieldName + ");}");
				} else
				if (fieldType.endsWith("MemoField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"); list.add(" + fieldName + ");}");
				} else
				if (fieldType.endsWith("NumField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"," + size + "," + dec + "); list.add("
							+ fieldName + ");}");
				} else
				if (fieldType.endsWith("PictureField")) {
					out.println("  if (" + fieldName + "==null) {" + fieldName //
							+ "=new " + fieldType + "(\"" + dbfFieldName + "\"); list.add(" + fieldName + ");}");
				} else {
					System.err.println("=== UNKNOWN FIELD TYPE: "+fieldType);
				}
			}
			out.println();
			out.println("  if (!list.isEmpty()) _dbf.addFields(list);");
			out.println("  attach(dbf); //make sure fields are really attached");
			for (Element element : elements) {
				String fieldName = element.getSimpleName().toString();
				out.println("  if (this." + fieldName + "==null) {\n"
						+ "    throw new java.lang.IllegalStateException(\"Field add to DBF failure: " //
						+ fieldName //
						+ "\");}\n");
			}
			out.println(" }");

			out.println(" public void append(" + dbfRecordSimpleClassName + " data) ");
			out.println("throws org.xBaseJ.xBaseJException, java.io.IOException {");
			out.println("  blank();");

			for (Element element : elements) {
				String fieldName = element.getSimpleName().toString();
				String fieldType = element.asType().toString();
				out.println("   // " + fieldType);

				final String methodSubname = methodSubname(fieldName);

				out.println("   set" + methodSubname + "(data.get" + methodSubname + "());");
				out.println();
			}

			out.println("  _dbf.write();");
			out.println("}");

			out.println(" public void setCharsetMapper(org.xBaseJ.cp.CharsetMapper mapper) ");
			out.println(" {");
			for (Element element : elements) {
				String fieldName = element.getSimpleName().toString();
				out.println("   this." + fieldName + ".setMapper(mapper);");
			}
			out.println("}");

			out.println();
			out.println("	@Override\n" + "	public java.util.Iterator<" + dbfRecordSimpleClassName
					+ "> iterator() {\n" + "		final org.xBaseJ.DBF dbf = this._dbf;\n"
					+ "		final int length = this._dbf.getRecordCount();\n"
					+ "		return new java.util.Iterator<>() {\n" + "			private int recno = 0;\n" + "\n"
					+ "			@Override\n" + "			public boolean hasNext() {\n"
					+ "				return length > recno;\n" + "			}\n" + "\n" + "			@Override\n"
					+ "			public " + dbfRecordSimpleClassName + " next() {\n" + "				recno++;\n"
					+ "				try {\n" + "					seek(recno);\n"
					+ "				} catch (org.xBaseJ.xBaseJException | java.io.IOException e) {\n"
					+ "					throw new java.lang.RuntimeException(e);\n" + "				}\n"
					+ "				return " + dbfRecordSimpleClassName + ".this;\n" + "			}\n" + "\n"
					+ "			@Override\n" + "			public void remove() {\n" + "				try {\n"
					+ "					seek(recno);\n" + "					dbf.delete();\n"
					+ "				} catch (org.xBaseJ.xBaseJException | java.io.IOException e) {\n"
					+ "					throw new java.lang.RuntimeException(e);\n" + "				}\n"
					+ "			}\n" + "		};\n" + "	}");
			out.println();

			out.println("}");
			out.println();
		}
	}

	private static String methodSubname(String fieldName) {
		return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1).toLowerCase();
	}

}
