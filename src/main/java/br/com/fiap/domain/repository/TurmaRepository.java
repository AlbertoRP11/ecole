package br.com.fiap.domain.repository;


import br.com.fiap.domain.entity.Turma;
import br.com.fiap.domain.infra.ConnectionFactory;
import org.w3c.dom.ls.LSOutput;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class TurmaRepository implements Repository<Turma,Long>{

    private CursoRepository cursoRepository = CursoRepository.build();
    private InstrutorRepository instrutorRepository = InstrutorRepository.build();


    private static AtomicReference<TurmaRepository> instance = new AtomicReference<>();

    private TurmaRepository(){
    }

    public static TurmaRepository build(){
        TurmaRepository result = instance.get();
        if (Objects.isNull(result)){
            TurmaRepository repository = new TurmaRepository();
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
    public List<Turma> findAll() {
        List<Turma> turmas = new ArrayList<>();


        try {
            var factory = ConnectionFactory.build();
            Connection connection = factory.getConnection();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM TURMA");

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    Long id = rs.getLong("TURMA_ID");
                    Date inicio = rs.getDate("INICIO");
                    Date encerramento = rs.getDate("ENCERRAMENTO");
                    var id_curso = rs.getInt("CURSO_ID");
                    var id_instrutor = rs.getInt("INSTRUTOR_ID");
                    var curso = cursoRepository.findById(id);
                    var instrutor = instrutorRepository.findById(id);
                    turmas.add(new Turma(id,curso,instrutor,null, inicio.toLocalDate(),encerramento.toLocalDate()));
                }
            }
            st.close();
            rs.close();
            connection.close();

        }
        catch (SQLException e){
            System.err.println( "Não foi possivel consultar os dados!\n" + e.getMessage() );
        }
        return turmas;
    }

    @Override
    public Turma findById(Long id) {
        Turma turma = null;
        var sql = "SELECT FROM TURMA WHERE TURMA_ID=?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1,id);
            ResultSet rs = ps.executeQuery();

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    Long id_turma = rs.getLong("TURMA_ID");
                    Date inicio = rs.getDate("INICIO");
                    Date encerramento = rs.getDate("ENCERRAMENTO");
                    var id_curso = rs.getInt("CURSO_ID");
                    var id_instrutor = rs.getInt("INSTRUTOR_ID");
                    var curso = cursoRepository.findById(id);
                    var instrutor = instrutorRepository.findById(id);
                    turma = new Turma(id_turma,curso,instrutor,null, inicio.toLocalDate(),encerramento.toLocalDate());

                }
            }
            else{
                System.out.println( "Turma não encontrada com o id = " + id );
            }
            rs.close();
            ps.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage());
        }
        return turma;
    }

    @Override
    public List<Turma> findByName(String texto) {
        List<Turma> turmas = new ArrayList<>();
        var sql = "SELECT * FROM cliente where UPPER(NM_CLIENTE) like ?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            texto = Objects.nonNull(texto) ? texto.toUpperCase() : "";
            ps.setString(1,"%"+texto+"%");
            ResultSet rs = ps.executeQuery();
            if (rs.isBeforeFirst()){
                while(rs.next()){
                    Long id = rs.getLong("TURMA_ID");
                    Date inicio = rs.getDate("INICIO");
                    Date encerramento = rs.getDate("ENCERRAMENTO");
                    var id_curso = rs.getInt("CURSO_ID");
                    var id_instrutor = rs.getInt("INSTRUTOR_ID");
                    var curso = cursoRepository.findById(id);
                    var instrutor = instrutorRepository.findById(id);
                    turmas.add(new Turma(id,curso,instrutor,null, inicio.toLocalDate(),encerramento.toLocalDate()));
                }
            }
            else {
                System.out.println( "Turma não encontrado com o nome = " + texto );
            }
            ps.close();
            rs.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage() );
        }
        return turmas;
    }

    @Override
    public Turma persist(Turma turma) {
        var sql = "INSERT INTO TURMA (CURSO_ID, INSTRUTOR_ID, INICIO, ENCERRAMENTO) "+
                "VALUES " +
                "(?, ?, ?, ?)";
        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();
        try{
            PreparedStatement ps = connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            ps.setInt(1, Integer.valueOf(String.valueOf(turma.getCurso().getId())));
            ps.setInt(2, Integer.valueOf(String.valueOf(turma.getInstrutor().getId())));
            ps.setDate(3,Date.valueOf(turma.getInicio()));
            ps.setDate(4,Date.valueOf(turma.getEncerramento()));
            int linhasAfetadas = ps.executeUpdate();
            if (linhasAfetadas > 0){
                System.out.println("turma matriculado com sucesso");
            }
            else{
                throw new SQLException("erro inesperado");
            }
            ps.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível matricular esse turma: \n" + e.getMessage() );
        }
        return turma;
    }
}