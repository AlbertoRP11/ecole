package br.com.fiap.domain.repository;

import br.com.fiap.domain.entity.Aluno;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AlunoRepository implements Repository<Aluno, Long>{

    private Set<Aluno> alunos;
    private static volatile AlunoRepository instance;
    private AlunoRepository(){
        alunos = new LinkedHashSet<>();
    }
    public static AlunoRepository of(){
        AlunoRepository result = instance;
        if(Objects.nonNull(result)){
            return result;
        }

        synchronized (AlunoRepository.class){
            if(Objects.isNull(null)){
                instance = new AlunoRepository();
            }
            return instance;
        }
    }
    @Override
    public List<Aluno> findAll() {
        return alunos.stream().toList();
    }

    @Override
    public Aluno findById(Long id) {
        return alunos.stream()
                .filter(a -> a.getId().equals(id))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<Aluno> findByName(String texto) {
        return alunos.stream()
                .filter(a -> a.getNome().toLowerCase().contains(texto.toLowerCase()))
                .toList();
    }

    @Override
    public Aluno persist(Aluno aluno) {
        if (Objects.isNull(aluno)) return null;
        if (Objects.isNull(aluno.getId() )) aluno.setId(alunos.size()+1L);
        alunos.add(aluno);
        return aluno;
    }
}
