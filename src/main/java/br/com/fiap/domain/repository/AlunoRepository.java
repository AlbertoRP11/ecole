package br.com.fiap.domain.repository;

import br.com.fiap.domain.entity.Aluno;
import br.com.fiap.domain.infra.ConnectionFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class AlunoRepository implements Repository<Aluno,Long> {

    private static AtomicReference<AlunoRepository> instance = new AtomicReference<>();

    private AlunoRepository(){
    }

    public static AlunoRepository build(){
        AlunoRepository result = instance.get();
        if (Objects.isNull(result)){
            AlunoRepository repository = new AlunoRepository();
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
    public List<Aluno> findAll() {
        List<Aluno> alunos = new ArrayList<>();

        try {
            var factory = ConnectionFactory.build();
            Connection connection = factory.getConnection();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM aluno");

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    Long id = rs.getLong("ALUNO_ID");
                    String nome = rs.getString("NOME");
                    String email = rs.getString("EMAIL");
                    String matricula = rs.getString("MATRICULA");
                    alunos.add(new Aluno(id,nome,matricula,email));
                }
            }
            st.close();
            rs.close();
            connection.close();

        }
        catch (SQLException e){
            System.err.println( "Não foi possivel consultar os dados!\n" + e.getMessage() );
        }
        return alunos;
    }

    @Override
    public Aluno findById(Long id) {
        Aluno aluno = null;
        var sql = "SELECT * FROM ALUNO WHERE ALUNO_ID=?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1,id);
            ResultSet rs = ps.executeQuery();

            if (rs.isBeforeFirst()){
                while (rs.next()){
                    var id_aluno = rs.getInt("ALUNO_ID");
                    var nome = rs.getString("NOME");
                    var email = rs.getString("EMAIL");
                    var matricula = rs.getString("MATRICULA");
                    Long idALunoLong = Long.valueOf(id_aluno);
                    aluno = new Aluno(idALunoLong,nome,matricula,email);
                }
            }
            else{
                System.out.println( "Aluno não encontrado com o id = " + id );
            }
            rs.close();
            ps.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage());
        }
        return aluno;
    }

    @Override
    public List<Aluno> findByName(String texto) {
        List<Aluno> alunos = new ArrayList<>();
        var sql = "SELECT * FROM ALUNO where UPPER(NOME) like ?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try{
            PreparedStatement ps = connection.prepareStatement(sql);
            texto = Objects.nonNull(texto) ? texto.toUpperCase() : "";
            ps.setString(1,"%"+texto+"%");
            ResultSet rs = ps.executeQuery();
            if (rs.isBeforeFirst()){
                while(rs.next()){
                    var id_aluno = rs.getInt("ALUNO_ID");
                    var nome = rs.getString("NOME");
                    var email = rs.getString("EMAIL");
                    var matricula = rs.getString("MATRICULA");
                    Long idALunoLong = Long.valueOf(id_aluno);
                    alunos.add(new Aluno(idALunoLong,nome,email,matricula));
                }
            }
            else {
                System.out.println( "Aluno não encontrado com o nome = " + texto );
            }
            ps.close();
            rs.close();
            connection.close();
        }
        catch (SQLException e){
            System.err.println( "Não foi possível executar a consulta: \n" + e.getMessage() );
        }
        return alunos;
    }

    @Override
    public Aluno persist(Aluno aluno) {
        var sql = "BEGIN" +
                " INSERT INTO aluno (NOME, EMAIL,MATRICULA) " +
                "VALUES (?,?,?) " +
                "returning ALUNO_ID into ?; " +
                "END;" +
                "";



        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();


        CallableStatement cs = null;
        try {
            cs = connection.prepareCall( sql );
            cs.setString( 1, aluno.getNome() );
            cs.setString(2, aluno.getEmail());
            cs.setString(3, aluno.getMatricula());
            cs.registerOutParameter( 4, Types.BIGINT );
            cs.executeUpdate();
            aluno.setId( cs.getLong( 4 ) );
            cs.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println( "Não foi possível executar o comando!\n" + e.getMessage() );
        }
        return aluno;
    }

}