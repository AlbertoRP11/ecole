package br.com.fiap.domain.repository;



import br.com.fiap.domain.entity.Curso;
import br.com.fiap.domain.infra.ConnectionFactory;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class CursoRepository implements Repository<Curso, Long>{

    private static AtomicReference<CursoRepository> instance = new AtomicReference<>();

    private CursoRepository(){
    }

    public static CursoRepository build(){
        CursoRepository result = instance.get();
        if (Objects.isNull(result)){
            CursoRepository repository = new CursoRepository();
            if (instance.compareAndSet(null,repository)){
                result = repository;
            }
            else {
                result = instance.get();
            }
        }
        return result;
    }
    @Override
    public List<Curso> findAll() {
        List<Curso> cursos = new ArrayList<>();

        try {
            var factory = ConnectionFactory.build();
            Connection connection = factory.getConnection();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM curso");

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    Long id = rs.getLong("CURSO_ID");
                    String nome = rs.getString("NOME");
                    int cargaHoraria = rs.getInt("CARGA_HORARIA");
                    Short cargaHorariaShort = Short.valueOf(String.valueOf(cargaHoraria));
                    cursos.add(new Curso(id,nome,cargaHorariaShort));
                }
            }
            st.close();
            rs.close();
            connection.close();

        }
        catch (SQLException e){
            System.err.println( "Não foi possivel consultar os dados!\n" + e.getMessage() );
        }
        return cursos;
    }

    @Override
    public Curso findById(Long id) {
        Curso curso = null;
        var sql = "SELECT * FROM CURSO WHERE CURSO_ID=?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1,id);
            ResultSet rs = ps.executeQuery();

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    var id_curso = rs.getInt("CURSO_ID");
                    var nome = rs.getString("NOME");
                    var cargaHoraria = rs.getShort("CARGA_HORARIA");
                    Long idCursoLong = Long.valueOf(id_curso);
                    curso = new Curso(idCursoLong,nome,cargaHoraria);
                }
            }
            else{
                System.out.println( "Curso não encontrado com o id = " + id );
            }
            rs.close();
            ps.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage());
        }
        return curso;
    }

    @Override
    public List<Curso> findByName(String texto) {
        List<Curso> cursos = new ArrayList<>();
        var sql = "SELECT * FROM CURSO where UPPER(NOME) like ?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            texto = Objects.nonNull(texto) ? texto.toUpperCase() : "";
            ps.setString(1,"%"+texto+"%");
            ResultSet rs = ps.executeQuery();
            if (rs.isBeforeFirst()){
                while(rs.next()){
                    var id = rs.getInt("CURSO_ID");
                    var nome = rs.getString("NOME");
                    var cargaHoraria = rs.getInt("CARGA_HORARIA");
                    Short cargaHorariaShort = Short.valueOf(String.valueOf(cargaHoraria));
                    Long idCursoLong = Long.valueOf(id);
                    cursos.add(new Curso(idCursoLong,nome,cargaHorariaShort));
                }
            }
            else {
                System.out.println( "Curso não encontrado com o nome = " + texto );
            }
            ps.close();
            rs.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage() );
        }
        return cursos;
    }

    @Override
    public Curso persist(Curso curso) {
        var sql = "BEGIN" +
                " INSERT INTO curso (NOME, CARGA_HORARIA) " +
                "VALUES (?,?) " +
                "returning CURSO_ID into ?; " +
                "END;" +
                "";
        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        CallableStatement cs = null;
        try {
            cs = connection.prepareCall( sql );
            cs.setString( 1, curso.getNome() );
            cs.setInt(2, curso.getCargaHoraria());
            cs.registerOutParameter( 3, Types.BIGINT );
            cs.executeUpdate();
            curso.setId( cs.getLong( 3 ) );
            cs.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println( "Não foi possível executar o comando!\n" + e.getMessage() );
        }
        return curso;
    }
}
