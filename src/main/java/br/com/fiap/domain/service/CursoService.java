package br.com.fiap.domain.service;

import br.com.fiap.domain.entity.Curso;
import br.com.fiap.domain.repository.CursoRepository;
import br.com.fiap.exception.MetodoNaoImplementadoException;

import java.util.List;

public class CursoService implements Service<Curso, Long>{
    private CursoRepository repository;

    public CursoService() {
        repository =  CursoRepository.of();
    }
    @Override
    public List<Curso> findAll() {

        return repository.findAll();
    }

    @Override
    public Curso findById(Long id) {
        return repository.findById(id);
    }

    @Override
    public List<Curso> findByName(String texto) {
        return repository.findByName(texto);
    }

    @Override
    public Curso persist(Curso curso) {
        return repository.persist(curso);
    }
}
