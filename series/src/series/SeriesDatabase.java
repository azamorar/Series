package series;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SeriesDatabase {
	private static Connection conn = null;

	/*
	 * Método auxiliar privado que cierra de forma segura y tratando los errores los argumentos.
	 * Los argumentos que no se deseen utilizar se inicializan a null.
	 */
	private void cierre(PreparedStatement pst, ResultSet rs, FileInputStream fis, Statement st) {
		// Cerrar el PreparedStatement
		try {
			if (pst != null) pst.close();
		} catch (SQLException e) {
			System.err.println("Error al cerrar la estructura pst: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar cerrar rs: ");
			System.err.println(t.getMessage());
		}

		// Cerrar el ResultSet
		try {
			if (rs != null) rs.close();
		} catch (SQLException e) {
			System.err.println("Error al cerrar la estructura rs: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar cerrar rs: ");
			System.err.println(t.getMessage());
		}

		// Cerrar el FileInputStream
		try {
			if (fis != null) fis.close();
		} catch (IOException e) {
			System.err.println("Error con el fichero: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar cerrar rs: ");
			System.err.println(t.getMessage());
		}

		// Cerrar Statement
		try {
			if (st != null) st.close();
		} catch (SQLException e) {
			System.err.println("Error al cerrar st: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar cerrar st: ");
			System.err.println(t.getMessage());
		}
	}
	/*
	 * Método constructor principal. No utilizado.
	 */
	public SeriesDatabase() { }

	/*
	 * El método abre la conexión con los parámetros indicados en el enunciado.
	 */
	public boolean openConnection() {

		try {
			if (conn == null || conn.isClosed()) {
				String address = "localhost:3306";		// dirección
				String database = "series";				// base de datos
				String user = "series_user";			// usuario
				String password = "series_pass";		// contraseña
				String url = "jdbc:mysql://" + address + "/" + database;
				conn = DriverManager.getConnection(url, user, password);
				System.out.println("Conexion abierta de forma exitosa");
				return true;
			}
		} catch (SQLException e) {
			System.err.println("Error al abrir la conexión: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar abrir la conexion: ");
			System.err.println(t.getMessage());
		}
		return false;
	}

	/*
	 * Cierra la conexión y trata los posibles errores y excepciones.
	 * Devulve falso si ha surgido algún problema. De otra forma, verdadero.
	 */
	public boolean closeConnection() {

		if (conn != null) {			// Comprueba que la conexión este abierta
			try {
				conn.close();
				return true;
			} catch (SQLException e) {
				System.err.println("La conexión no se ha cerrado: ");
				System.err.println(e.getMessage());
			} catch (Exception t) {
				System.err.println("Otro tipo de error al intentar cerrar la conexion: ");
				System.err.println(t.getMessage());
			}
		}

		return false;
	}

	/*
	 * Crea la tabla capítulo, y devuelve falso si surge algún problema, y si no, verdadero.
	 */
	public boolean createTableCapitulo() {

		this.openConnection();		// Llamada a método para abrir la conexión
		Statement st = null;
		ResultSet rs = null;
		String query = "CREATE TABLE capitulo ("			 	+
				" n_orden INT," 								+
				" titulo VARCHAR(100)," 						+
				" duracion INT," 								+ 
				" fecha_estreno DATE," 							+
				" n_temporada INT," 							+
				" id_serie INT," 								+
				" PRIMARY KEY (n_orden,id_serie,n_temporada)," 	+
				" FOREIGN KEY (id_serie,n_temporada) REFERENCES temporada(id_serie,n_temporada) ON DELETE CASCADE ON UPDATE CASCADE);";


		try {
			DatabaseMetaData dbmeta = conn.getMetaData();		// Metadata de la base de datos
			rs = dbmeta.getTables(null, null, "capitulo", null);		// Iterable con las tablas de la base de dato cuyo titulo es 'capitulo'
			if (rs.next()) {		// Comprueba si la tabla 'capitulo' ya existe
				System.err.println("La tabla 'capitulo' ya fue creada");
				return false;
			}
			st = conn.createStatement();
			st.executeUpdate(query);
			return true;
		} catch (SQLException e) {
			System.err.println("Error al crear la tabla capitulo: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar crear la tabla capitulo: ");
			System.err.println(t.getMessage());
		} finally {
			cierre(null, rs, null, st);
		}

		return false;
	}

	/*
	 * Crea la tabla valora y devuelve verdadero o falso si surge alguna excepción.
	 */
	public boolean createTableValora() {

		this.openConnection(); 		// Llamada a método para abrir la conexión
		ResultSet rs = null;
		Statement st = null;
		String query = "CREATE TABLE valora (" 	+
				" fecha DATE,"					+
				" valor INT," 					+
				" id_usuario INT," 				+
				" n_orden INT," 				+
				" n_temporada INT," 			+
				" id_serie INT," 				+
				" PRIMARY KEY (fecha, id_usuario, n_orden, n_temporada, id_serie)," 							 +
				" FOREIGN KEY (id_usuario) REFERENCES usuario (id_usuario) ON DELETE CASCADE ON UPDATE CASCADE," +
				" FOREIGN KEY (id_serie, n_temporada, n_orden) REFERENCES capitulo(id_serie, n_temporada, n_orden) ON DELETE CASCADE ON UPDATE CASCADE" +
				");";



		try {
			DatabaseMetaData dbmeta = conn.getMetaData();		// Metadata de la base de datos
			rs = dbmeta.getTables(null, null, "valora", null);		// Iterable con las tablas de la base de dato cuyo titulo es 'valora'
			if (rs.next()) {		// Comprueba si la tabla 'valora' ya existe
				System.err.println("La tabla 'valora' ya fue creada");
				return false;
			}
			st = conn.createStatement();
			st.executeUpdate(query);
			return true;
		} catch (SQLException e) {
			System.err.println("Error al crear la tabla valora: ");
			System.err.println(e.getMessage());
		} catch (Exception t) {
			System.err.println("Otro tipo de error al intentar crear la tabla valora: ");
			System.err.println(t.getMessage());
		} finally {
			cierre(null, rs, null, st);
		}
		return false; 
	}


	/*
	 * Añade las filas del fichero que se pasa como parámetro y devuelve el número de filas añadidas.
	 * Suponemos que el fichero de entrada no tiene errores de que falten columnas o elementos. 
	 */
	public int loadCapitulos(String fileName) {

		this.openConnection();		// Llamada a método para abrir la conexión
		int contador = 0;	// Almacena el número de filas metidas en la tabla
		PreparedStatement pst = null;


		// Trata toda la operación como un solo bloque.
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			System.err.println("Error de sql: ");
			System.err.println(e1.getMessage());
		}catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		}

		try { 
			// Este código pasa línea por línea (excluyendo la primera línea, donde se almacena el nombre de las columnas) y las 
			// introduce en la tabla.
			String line = "";  
			String splitBy = ";";  
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
			String [] info = line.split(splitBy);
			String query = "INSERT INTO capitulo(" + info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + info[4] + "," + info[5] + ") VALUES (?,?,?,?,?,?);";
			while ((line = br.readLine()) != null) {  
				pst = conn.prepareStatement(query);
				String[] capitulos = line.split(splitBy);
				// Aunque el cast a int no es necesario, lo ponemos por precaución
				pst.setInt(1, (int) Integer.parseInt(capitulos[0]));
				pst.setInt(2, (int) Integer.parseInt(capitulos[1]));
				pst.setInt(3, (int) Integer.parseInt(capitulos[2]));
				pst.setString(4, capitulos[3]);
				pst.setString(5, capitulos[4]);
				pst.setInt(6, (int) Integer.parseInt(capitulos[5]));

				contador += pst.executeUpdate();
			}

			// Cierra el fichero de entrada
			try {
				if (br != null) br.close();
			} catch (IOException e) {
				System.err.println("Error al cerrar br: ");
				System.err.println(e.getMessage());
			}catch (Exception e) {
				System.err.println("Otro tipo de Error: ");
				System.err.println(e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error al hacer el setInt: ");
			System.err.println(e.getMessage());
		} catch (IOException e) {  
			System.err.println("Error al leer el .csv: ");
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		} finally {
			cierre(pst,null,null,null);
		}

		// Fin del bloque
		try {
			conn.commit();
		} catch (SQLException e) {
			System.err.println("Error de SQL: ");
			System.err.println(e.getMessage());
		}

		return contador;
	}

	/*
	 * Introduce en la tabla valora los elementos del fichero pasado como parámetro.
	 * Suponemos que el fichero de entrada no tiene errores de que falten columnas o elementos. 
	 */
	public int loadValoraciones(String fileName) {

		this.openConnection();		// Llamada a método para abrir la conexión
		int contador = 0;
		PreparedStatement pst = null;

		try {
			// Línea por línea introduce los elementos
			String line = "";  
			String splitBy = ";";  
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
			String [] info = line.split(splitBy);
			String query = "INSERT INTO valora(" + info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + info[4] + "," + info[5] + ") VALUES (?,?,?,?,?,?);";
			while ((line = br.readLine()) != null) {  
				pst = conn.prepareStatement(query);
				String[] capitulos = line.split(splitBy); 
				pst.setInt(1, (int) Integer.parseInt(capitulos[0]));
				pst.setInt(2, (int) Integer.parseInt(capitulos[1]));
				pst.setInt(3, (int) Integer.parseInt(capitulos[2]));
				pst.setInt(4, (int) Integer.parseInt(capitulos[3]));
				pst.setString(5, capitulos[4]);
				pst.setInt(6, (int) Integer.parseInt(capitulos[5]));

				contador += pst.executeUpdate();
			}
			try {
				if (br != null) br.close();
			} catch (IOException e) {
				System.err.println("Error al cerrar br: ");
				System.err.println(e.getMessage());
			}catch (Exception e) {
				System.err.println("Otro tipo de Error: ");
				System.err.println(e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error al hacer el setInt: ");
			System.err.println(e.getMessage());
		} catch (IOException e) {  
			System.err.println("Error al leer el .csv: ");
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		} finally {
			cierre(pst,null,null,null);
		}

		return contador;

	}

	/*
	 * Devuelve en el formato especificado los elementos pedidos.
	 */
	public String catalogo() {

		this.openConnection();		// Llamada a método para abrir la conexión

		ResultSet rs = null;
		String query = 	"SELECT titulo,n_capitulos " 				+
				"FROM serie s " 									+
				"INNER JOIN temporada t ON s.id_serie=t.id_serie " 	+
				"ORDER BY s.id_serie ASC, n_temporada ASC;"			;

		boolean primero = true;
		String resultado = "{";
		Statement st = null;

		try {
			String aux = null;
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				if (primero) {
					resultado += rs.getString(1) + ":[" + rs.getString(2);
					primero = false;
				}
				else if (aux.equals(rs.getString(1)))
					resultado += "," + rs.getString(2);
				else 
					resultado += "]," + rs.getString(1) + ":[" + rs.getString(2);
				aux = rs.getString(1);
			}
		} catch (SQLException e) {
			System.err.println("Error de SQL: ");
			System.err.println(e.getMessage());
			return null;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
			return null;
		}finally {
			cierre(null, rs, null, st);
		}

		return resultado + (!primero ? "]" : "") + "}";
	}

	/*
	 * Devuelve una lista en el formato especificado de los usuarios que no han comentado en una serie.
	 */
	public String noHanComentado() {

		this.openConnection();		// Llamada a método para abrir la conexión
		ResultSet rs = null;
		String query =	"SELECT nombre,apellido1,apellido2 FROM usuario u " +
				"LEFT JOIN comenta c ON u.id_usuario=c.id_usuario "			+
				"WHERE texto IS NULL " 										+
				"ORDER BY apellido1 ASC,apellido2 ASC, nombre ASC;"			;

		String resultado = "[";

		Statement st = null;

		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			boolean primero = true;
			while (rs.next()) {
				if (primero == true) {
					resultado += rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3);
					primero = false;
				}
				else resultado += "," + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3);

			}
		} catch (SQLException e) {
			System.err.println("Error de SQL: ");
			System.err.println(e.getMessage());
			return null;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
			return null;
		}
		finally {
			cierre(null, rs, null, st);
		}

		return resultado + "]";
	}

	/*
	 * Devuelve una media de las valoraciones de los capítulos de las series cuyos géneros sean
	 * los del parámetro de entrada.
	 */
	public double mediaGenero(String genero) {

		this.openConnection();		// Llamada a método para abrir la conexión
		ResultSet rs = null;
		ResultSet rs1 = null;
		Statement st = null;
		PreparedStatement pst = null;
		boolean encontrado = false;
		double res = -1.0;
		String query = 	"SELECT AVG(valor) FROM genero g " 					+
				"INNER JOIN pertenece p ON g.id_genero=p.id_genero "	+
				"INNER JOIN serie s ON p.id_serie=s.id_serie " 			+
				"INNER JOIN comenta cm ON s.id_serie=cm.id_serie " 		+
				"INNER JOIN usuario u ON cm.id_usuario=u.id_usuario " 	+
				"INNER JOIN valora v ON u.id_usuario=v.id_usuario " 	+
				"WHERE descripcion=?; "									;

		String query0 = "SELECT descripcion FROM genero;";
		
		
		try {
			st = conn.createStatement();
			rs1 = st.executeQuery(query0);
			while (rs1.next() && encontrado == false) {
				if (rs1.getString(1).equals(genero)) {
					res = 0.0;
					encontrado = true;
				}
			}
			pst = conn.prepareStatement(query);
			pst.setString(1, genero);
			rs = pst.executeQuery();
		} catch (SQLException e) {
			System.err.println("No se ha podido ejecutar el query: ");
			System.err.println(e.getMessage());
			cierre(pst, rs, null, null);
			return -2.0;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		}

		try {
			rs.next();
			if (rs.getObject("AVG(valor)") != null) res = rs.getDouble("AVG(valor)");
		} catch (SQLException e) {
			System.err.println("No se ha podido conseguir el elemento de la tabla: ");
			System.err.println(e.getMessage());
			return -2.0;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		} finally {
			cierre(pst, rs, null, st);
			try {
				if (rs1 != null) rs1.close();
			} catch (SQLException e) {
				System.err.println("Error al cerrar la estructura rs1: ");
				System.err.println(e.getMessage());
			} catch (Exception t) {
				System.err.println("Otro tipo de error al intentar cerrar rs1: ");
				System.err.println(t.getMessage());
			}
		}

		return res;
	}

	/*
	 * Devuelve la duración media de los capítulos cuyo idioma sea el parámetro de entrada.
	 */
	public double duracionMedia(String idioma) {

		this.openConnection();		// Llamada a método para abrir la conexión
		ResultSet rs = null;
		double res = -1.0;
		String query = "SELECT AVG(duracion) FROM serie s " 										+			 
				"INNER JOIN temporada t ON s.id_serie=t.id_serie " 									+
				"INNER JOIN capitulo c ON s.id_serie=c.id_serie AND t.n_temporada=c.n_temporada " 	+
				"LEFT JOIN valora v ON c.n_orden=v.n_orden " 										+
				"WHERE idioma=? AND valor IS NULL;"													;



		PreparedStatement pst = null;
		try {
			pst = conn.prepareStatement(query);
			pst.setString(1, idioma);
			rs = pst.executeQuery();
		} catch (SQLException e) {
			System.err.println("No se ha podido ejecutar el query: ");
			System.err.println(e.getMessage());
			cierre(pst, rs, null, null);
			return -2.0;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		}

		try {
			rs.next();
			if (rs.getObject("AVG(duracion)") != null) res = rs.getDouble("AVG(duracion)");

		} catch (SQLException e) {
			System.err.println("No se ha podido conseguir el elemento de la tabla: ");
			System.err.println(e.getMessage());
			return -2.0;
		} catch (Exception e) {
			System.err.println("Otro tipo de Error: ");
			System.err.println(e.getMessage());
		} finally {
			cierre(pst, rs, null, null);
		}

		return res;
	}

	/* 
	 * Pone la foto de entrada al usuario cuyo primer apellido es Cabeza
	 */
	public boolean setFoto(String filename) {

		this.openConnection();		// Llamada a método para abrir la conexión
		PreparedStatement pst = null;
		File f = null;
		FileInputStream fis = null;

		try {
			pst = conn.prepareStatement("UPDATE usuario SET fotografia = ? WHERE apellido1 = 'Cabeza' AND fotografia IS NULL;");
			f = new File(filename);
			fis = new FileInputStream(f);

			pst.setBinaryStream(1, fis, (int) f.length());

			int n = pst.executeUpdate();
			if (n == 1) {
				System.out.println("Foto añadida a un usuario");
				return true;
			} 
			
			else System.out.println("Foto no añadida");

		} catch (SQLException e) {
			System.err.println("Error SQL al añadir una foto: ");
			System.err.println(e.getMessage());
		} catch (FileNotFoundException e) {
			System.err.println("Error de foto no encontrada: ");
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.println("Error al añadir la foto: ");
			System.err.println(e.getMessage());
		} finally {
			cierre(pst, null, fis, null);
		}
		return false;
	}

}
