/*
 * data-mover
 * https://github.com/parroit/data-mover
 *
 * Copyright (c) 2013 parroit
 * Licensed under the MIT license.
 */

'use strict';

var ecmModel = require("ecm-model");
var sqlite3 = require('sqlite3').verbose();

var users = {};

function import_users(models, next) {
    var saved = 0;
    var notSaved = 0;
    var total = 0;
    models.User.remove({}, function (err) {
        console.log('all users removed');


        db.each(
            "SELECT * FROM users",
            function (err, row) {
                var user = new models.User(row);

                ['nome', 'cognome', 'luogo_nascita'].forEach(function (f) {
                    row[f] = row[f] || "NON SPECIFICATO";
                });


                users[row.name] = {
                    name: row.name,
                    nome: row.nome,
                    cognome: row.cognome,
                    codice_fiscale: row.codice_fiscale,
                    farmacia: row.farmacia
                };
                user.save(function (err) {
                    if (err) {
                        console.log("User %s cannot be saved to mongodb:%s", user.name, err);
                        notSaved++;
                    }

                    else {
                        console.log("User %d saved:%s", saved, user.name);
                        saved++;
                    }
                    if (saved + notSaved == total) {
                        console.log("Total users retrieved %d", total);
                        console.log("Total users saved %d", saved);
                        console.log("Total users in error %d", notSaved);
                        next && next();
                    }
                });


            },
            function (err, totalrows) {
                total = totalrows

            }
        );
    });
}
var examsIds = {};
function import_exams(models, next) {
    models.Exam.remove({}, function (err) {
        console.log('all exams removed');

        var examSaved = 0;
        var examNotSaved = 0;
        var examTotal = 0;
        db.each(
            "SELECT * FROM exams",
            function (err, exam_row) {
                examsIds[exam_row.id] = exam_row.codice;
                var ex = new models.Exam(exam_row);
                (function (exam) {
                    db.each(
                        "SELECT * FROM questions where exam = ?", exam_row.id,
                        function (err, question_row) {

                            exam.questions.push(question_row);
                        },
                        function () {
                            exam.save(function (err) {
                                if (err) {
                                    console.log("Exam %s cannot be saved to mongodb:%s", exam.id, err);
                                    examNotSaved++;
                                }

                                else {
                                    console.log("Exam %d saved:%s", examSaved, exam.id);
                                    examSaved++;
                                }
                                if (examSaved + examNotSaved == examTotal) {
                                    console.log("Total exam retrieved %d", examTotal);
                                    console.log("Total exam saved %d", examSaved);
                                    console.log("Total exam in error %d", examNotSaved);
                                    next && next();
                                }
                            });
                        }
                    );
                })(ex);


            },
            function (err, totalrows) {
                examTotal = totalrows;

            }
        );
    });
}

var takenExamsIds = {};

function import_taken_exams(models, next) {
    models.TakenExam.remove({}, function (err) {
        console.log('all takenExam removed');

        var examSaved = 0;
        var examNotSaved = 0;
        var examTotal = 0;
        db.each(
            "SELECT * FROM taken_exams",
            function (err, exam_row) {
                exam_row.exam_id = examsIds[exam_row.exam_id];
                takenExamsIds[exam_row.id] = {
                    user_id: exam_row.user_id,
                    exam_id: exam_row.exam_id
                };
                var user = users[exam_row.user_id];
                if (user) {
                    exam_row.user_nome = user.nome;
                    exam_row.user_cognome = user.cognome;
                    exam_row.user_codice_fiscale = user.codice_fiscale;
                    exam_row.user_farmacia = user.farmacia;

                } else {
                    exam_row.user_nome = "";
                    exam_row.user_cognome = "";
                    exam_row.user_codice_fiscale = "";
                    exam_row.user_farmacia = "";

                }

                var te = new models.TakenExam(exam_row);

                te.save(function (err) {
                    if (err) {
                        console.log("TakenExam %s cannot be saved to mongodb:%s", te.id, err);
                        examNotSaved++;
                    }

                    else {
                        console.log("TakenExam %d saved:%s", examSaved, te.id);
                        examSaved++;
                    }
                    if (examSaved + examNotSaved == examTotal) {
                        console.log("Total TakenExam retrieved %d", examTotal);
                        console.log("Total TakenExam saved %d", examSaved);
                        console.log("Total TakenExam in error %d", examNotSaved);
                        next && next();
                    }
                });


            },
            function (err, totalrows) {
                examTotal = totalrows;

            }
        );
    });
}

function import_taken_questions(models, next) {
    models.TakenQuestion.remove({}, function (err) {
        console.log('all takenQuestion removed');

        var questionSaved = 0;
        var questionNotSaved = 0;
        var questionTotal = 0;


        var questionIds = [];
        db.all(
            "SELECT id FROM taken_questions order by id",
            function (err, ids) {
                if (err) throw err;
                questionTotal = ids.length;
                console.log("Found %d taken questions", questionTotal);
                createIds(0, ids);


            }
        );

        function createIds(idx, ids) {
            var number = 200;
            var from = ids[idx * number];
            var toIdx = Math.min(((idx + 1) * number) - 1, ids.length - 1);
            var to = ids[ toIdx];

            questionIds.push({
                from: from.id,
                to: to.id
            });

            if ((idx + 1) * number < ids.length - 1) {
                createIds(idx + 1, ids);
            } else {
                loadPartials(0);
            }

        }

        function loadPartials(idx) {

            var documentSaved = 0;
            db.each(
                "SELECT * FROM taken_questions where id >= ? and id <= ?",
                [ questionIds[idx].from, questionIds[idx].to],
                function (err, question_row) {
                    var exam = takenExamsIds[question_row.exam_id];
                    if (exam) {
                        question_row.exam_id = exam.exam_id;
                        question_row.user_id = exam.user_id;
                    } else {
                        question_row.exam_id ="";
                        question_row.user_id ="";
                        console.log(question_row.id + " exam unknow");
                    }


                    var takenQuestion = new models.TakenQuestion(question_row);

                    takenQuestion.save(function (err) {
                        if (err) {
                            console.log("TakenQuestion %s cannot be saved to mongodb:%s", takenQuestion.id, err);
                            questionNotSaved++;
                        }


                        else {
                            if (questionSaved % 1000 == 0)
                                console.log("TakenQuestion %d saved:%s", questionSaved, takenQuestion.id);
                            questionSaved++;
                        }

                        documentSaved++;

                        if (documentSaved == 200) {
                            loadPartials(idx + 1);
                        }

                    });

                },

                function (err, totalrows) {

                    if (totalrows < 200) {
                        console.log("Last set loaded, waiting for documents to save...");
                        setTimeout(function () {
                            if (questionSaved + questionNotSaved == questionTotal) {
                                console.log("Total TakenQuestion retrieved %d", questionTotal);
                                console.log("Total TakenQuestion saved %d", questionSaved);
                                console.log("Total TakenQuestion in error %d", questionNotSaved);
                                next && next();
                            } else {
                                console.log("ERROR! Total TakenQuestion retrieved %d", questionTotal);
                                console.log("ERROR! Total TakenQuestion saved %d", questionSaved);
                                console.log("ERROR! Total TakenQuestion in error %d", questionNotSaved);
                            }
                        }, 10000);


                    }

                }

            );
        }
    });
}

var db = new sqlite3.Database('../old-data/ecm.db', sqlite3.OPEN_READWRITE, function (err) {
    if (err) throw err;


    ecmModel.connect('mongodb://localhost/ecm', function (schemdas, models) {


        var fs = require("fs");

        console.log("Starting");


        import_exams(models, function () {
            import_users(models, function () {
                import_taken_exams(models, function () {
                    import_taken_questions(models, function () {

                        console.log("IMPORT SUCCESSFULLY");
                    });
                });

            })
        });

    });


});
